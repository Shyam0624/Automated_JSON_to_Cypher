import json
import os
import re
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Set, Tuple
from collections import defaultdict, deque

# =========================
# Pydantic Models
# =========================

class Condition(BaseModel):
    field: str
    operator: str
    value: Any

class WhereClause(BaseModel):
    type: str
    conditions: List[Condition]

class Node(BaseModel):
    label: str
    alias: str

class Relationship(BaseModel):
    node1: str
    node2: str
    type: str
    optional: Optional[bool] = False

class Aggregation(BaseModel):
    function: str
    field: str
    alias: str

class WithClause(BaseModel):
    fields: List[str]
    aggregations: Optional[List[Aggregation]] = None

class ReturnClause(BaseModel):
    fields: List[str]
    distinct: bool = False

class OrderByClause(BaseModel):
    field: str
    direction: str

class CypherQuery(BaseModel):
    nodes: List[Node]
    relationships: List[Relationship]
    whereClause: Optional[WhereClause] = None
    with_: Optional[WithClause] = Field(None, alias="with")
    withClause: Optional[WhereClause] = None
    returnClause: ReturnClause = Field(..., alias="return")
    orderBy: Optional[OrderByClause] = None
    limit: Optional[int] = None

# =========================
# Validation Functions
# =========================

def validate_aliases(nodes: List[Node], relationships: List[Relationship]):
    """Ensure all relationship aliases exist in the nodes list."""
    node_aliases = {n.alias for n in nodes}
    for rel in relationships:
        if rel.node1 not in node_aliases:
            raise ValueError(f"Relationship node1 alias '{rel.node1}' not found in nodes.")
        if rel.node2 not in node_aliases:
            raise ValueError(f"Relationship node2 alias '{rel.node2}' not found in nodes.")

def validate_field_syntax(fields: List[str]):
    """Ensure fields with spaces are properly backticked."""
    for field in fields:
        if " " in field and not re.match(r"^.*`.*`.*$", field):
            raise ValueError(f"Field '{field}' contains spaces and is not backticked.")

def validate_conditions(conditions: List[Condition]):
    """Apply field syntax validation to all WHERE conditions."""
    for cond in conditions:
        validate_field_syntax([cond.field])

# =========================
# Cypher Pattern Helpers
# =========================

def _fmt_node(alias: str, node_lookup: Dict[str, Node]) -> str:
    n = node_lookup.get(alias)
    return f"({alias}:{n.label})" if n else f"({alias})"

def _fmt_node_with_props(alias: str, label: str, props: Dict[str, Any]) -> str:
    if not props:
        return f"({alias}:{label})"
    assignments = []
    for k, v in props.items():
        if isinstance(v, str):
            assignments.append(f"{k}: '{v}'")
        elif isinstance(v, bool):
            assignments.append(f"{k}: {str(v).lower()}")
        elif v is None:
            assignments.append(f"{k}: null")
        else:
            assignments.append(f"{k}: {v}")
    return f"({alias}:{label} {{{', '.join(assignments)}}})"

def _connected_components_on_rels(rel_pairs: List[Tuple[str, str, str]]) -> List[List[int]]:
    """Find connected components among relationships for pattern chaining."""
    adj: Dict[int, Set[int]] = defaultdict(set)
    for i, (a1, a2, _) in enumerate(rel_pairs):
        for j, (b1, b2, _) in enumerate(rel_pairs):
            if i == j:
                continue
            if a1 == b1 or a1 == b2 or a2 == b1 or a2 == b2:
                adj[i].add(j)
                adj[j].add(i)
    seen: Set[int] = set()
    comps: List[List[int]] = []
    for i in range(len(rel_pairs)):
        if i in seen:
            continue
        dq = deque([i])
        seen.add(i)
        comp = []
        while dq:
            u = dq.popleft()
            comp.append(u)
            for v in adj[u]:
                if v not in seen:
                    seen.add(v)
                    dq.append(v)
        comps.append(comp)
    return comps

def _chain_component(rel_pairs: List[Tuple[str, str, str]],
                     comp_idxs: List[int],
                     node_lookup: Dict[str, Node],
                     preferred_start: Optional[str] = None) -> str:
    """Chain a component of relationships into a single pattern."""
    from collections import defaultdict as _dd
    comp_edges = [rel_pairs[i] for i in comp_idxs]
    degree = _dd(int)
    for a, b, _ in comp_edges:
        degree[a] += 1
        degree[b] += 1
    aliases_in_comp = {a for a, _, _ in comp_edges} | {b for _, b, _ in comp_edges}
    if preferred_start in aliases_in_comp:
        start = preferred_start
    else:
        start = next((alias for alias, d in degree.items() if d == 1), comp_edges)

    alias_adj: Dict[str, List[int]] = defaultdict(list)
    for idx, (a, b, _) in enumerate(comp_edges):
        alias_adj[a].append(idx)
        alias_adj[b].append(idx)

    used = [False] * len(comp_edges)
    segs: List[str] = []
    segs.append(_fmt_node(start, node_lookup))
    current = start
    used_count = 0

    while used_count < len(comp_edges):
        progressed = False
        for ei in alias_adj[current]:
            if used[ei]:
                continue
            a, b, rt = comp_edges[ei]
            if a == current:
                arrow = f"-[:{rt}]->"
                nxt = b
            else:
                arrow = f"<-[:{rt}]-"
                nxt = a
            segs.append(arrow)
            segs.append(_fmt_node(nxt, node_lookup))
            used[ei] = True
            used_count += 1
            current = nxt
            progressed = True
            break
        if not progressed:
            simples = []
            for a, b, rt in comp_edges:
                la = _fmt_node(a, node_lookup)
                lb = _fmt_node(b, node_lookup)
                simples.append(f"{la}-[:{rt}]->{lb}")
            return ", ".join(simples)
    return "".join(segs)

def _build_single_match_clause(nodes: List[Node], relationships: List[Relationship]) -> str:
    """Build a single MATCH clause for all non-optional relationships."""
    regular = [rel for rel in relationships if not getattr(rel, "optional", False)]
    node_lookup = {n.alias: n for n in nodes}
    if not regular:
        if nodes:
            node_patterns = [f"({n.alias}:{n.label})" for n in nodes]
            return f"MATCH {', '.join(node_patterns)}"
        return ""
    rel_pairs: List[Tuple[str, str, str]] = [(rel.node1, rel.node2, rel.type) for rel in regular]
    comps = _connected_components_on_rels(rel_pairs)
    patterns = [_chain_component(rel_pairs, comp, node_lookup, preferred_start=None) for comp in comps]
    involved = set()
    for a, b, _ in rel_pairs:
        involved.add(a); involved.add(b)
    standalone = [f"({n.alias}:{n.label})" for n in nodes if n.alias not in involved]
    if standalone:
        patterns.extend(standalone)
    return f"MATCH {', '.join(patterns)}"

def _extract_anchor_from_where(where: Optional[WhereClause], node_lookup: Dict[str, Node]) -> Optional[Tuple[str, str, Dict[str, Any]]]:
    """Extract anchor node info from WHERE clause if possible."""
    if not where or not where.conditions:
        return None
    props_by_alias: Dict[str, Dict[str, Any]] = defaultdict(dict)
    for cond in where.conditions:
        field = cond.field.strip()
        if '.' not in field:
            continue
        alias, prop = field.split('.', 1)
        alias = alias.strip()
        prop = prop.strip().strip('`')
        if alias in node_lookup and cond.operator == '=':
            props_by_alias[alias][prop] = cond.value
    if len(props_by_alias) == 1:
        anchor_alias = next(iter(props_by_alias))
        return (anchor_alias, node_lookup[anchor_alias].label, props_by_alias[anchor_alias])
    return None

def _extract_return_aliases(ret: ReturnClause) -> List[str]:
    """Extract all aliases used in RETURN fields."""
    aliases = []
    for f in ret.fields:
        part = f.split(' AS ')[0].strip()
        if '.' in part:
            aliases.append(part.split('.', 1)[0].strip())
        else:
            aliases.append(part)
    return list(dict.fromkeys(aliases))

def _build_optional_match_components(nodes: List[Node],
                                     optional_rels: List[Relationship],
                                     bound_aliases: Set[str]) -> List[str]:
    """Build OPTIONAL MATCH clauses for optional relationships."""
    node_lookup = {n.alias: n for n in nodes}
    opt_pairs = [(rel.node1, rel.node2, rel.type) for rel in optional_rels]
    comps = _connected_components_on_rels(opt_pairs)
    lines: List[str] = []
    for comp in comps:
        comp_edges = [opt_pairs[i] for i in comp]
        aliases_in_comp = set()
        for a, b, _ in comp_edges:
            aliases_in_comp.add(a); aliases_in_comp.add(b)
        start_alias = next((x for x in aliases_in_comp if x in bound_aliases), None)
        pattern = _chain_component(opt_pairs, comp, node_lookup, preferred_start=start_alias)
        lines.append(f"OPTIONAL MATCH {pattern}")
    return lines

# =========================
# Advanced Pattern Builder
# =========================

class AdvancedPatternBuilder:
    """Builds optimal Cypher patterns with MATCH and OPTIONAL MATCH."""
    def __init__(self, nodes: List[Node], relationships: List[Relationship],
                 return_clause: ReturnClause, where_clause: Optional[WhereClause] = None):
        self.node_lookup = {node.alias: node for node in nodes}
        self.nodes = nodes
        self.relationships = relationships
        self.regular_rels = [rel for rel in relationships if not rel.optional]
        self.optional_rels = [rel for rel in relationships if rel.optional]
        self.return_clause = return_clause
        self.where_clause = where_clause

    def build_optimal_patterns(self) -> List[str]:
        patterns: List[str] = []
        bound_aliases: Set[str] = set()
        node_lookup = self.node_lookup

        if self.regular_rels:
            match_line = _build_single_match_clause(self.nodes, self.relationships)
            if match_line:
                patterns.append(match_line)
            for rel in self.regular_rels:
                bound_aliases.add(rel.node1); bound_aliases.add(rel.node2)

        anchor_info = None
        if not self.regular_rels:
            anchor_info = _extract_anchor_from_where(self.where_clause, node_lookup)
            if anchor_info:
                a_alias, a_label, a_props = anchor_info
                patterns.append(f"MATCH {_fmt_node_with_props(a_alias, a_label, a_props)}")
                bound_aliases.add(a_alias)
            else:
                ret_aliases = [a for a in _extract_return_aliases(self.return_clause) if a in node_lookup]
                unique_ret = list(dict.fromkeys(ret_aliases))
                chosen = None
                for p in ['j', 'Job', 'c', 'Candidate', 'r', 'Resume', 's', 'Skill']:
                    if p in unique_ret:
                        chosen = p
                        break
                if not chosen and len(unique_ret) == 1:
                    chosen = unique_ret[0]
                if chosen:
                    a_alias = chosen
                    a_label = node_lookup[a_alias].label
                    patterns.append(f"MATCH ({a_alias}:{a_label})")
                    bound_aliases.add(a_alias)

        if self.optional_rels:
            opt_lines = _build_optional_match_components(self.nodes, self.optional_rels, bound_aliases)
            patterns.extend(opt_lines)

        if len(self.relationships) == 0 and not anchor_info and not bound_aliases and self.nodes:
            standalone = ", ".join([f"({n.alias}:{n.label})" for n in self.nodes])
            patterns.append(f"MATCH {standalone}")

        return [p for p in patterns if p]

# =========================
# Cypher Generation Functions
# =========================

def build_advanced_match_clause(nodes: List[Node], relationships: List[Relationship],
                                return_clause: ReturnClause, where_clause: Optional[WhereClause] = None) -> str:
    """Builds the MATCH/OPTIONAL MATCH part of the Cypher query."""
    if relationships is None or len(relationships) == 0:
        if nodes:
            node_patterns = [f"({node.alias}:{node.label})" for node in nodes]
            return f"MATCH {', '.join(node_patterns)}"
        return ""
    builder = AdvancedPatternBuilder(nodes, relationships, return_clause, where_clause)
    patterns = builder.build_optimal_patterns()
    return "\n".join(patterns)

def build_where_clause(where_clause: WhereClause) -> str:
    """Builds the WHERE clause."""
    conditions = []
    for cond in where_clause.conditions:
        if isinstance(cond.value, str):
            value = f"'{cond.value}'"
        elif isinstance(cond.value, bool):
            value = str(cond.value).lower()
        elif cond.value is None:
            value = "null"
        else:
            value = str(cond.value)
        conditions.append(f"{cond.field} {cond.operator} {value}")
    logical_op = f" {where_clause.type} "
    return f"WHERE {logical_op.join(conditions)}"

def build_with_clause(with_clause: WithClause) -> str:
    """Builds the WITH clause."""
    parts = with_clause.fields.copy()
    if with_clause.aggregations:
        for agg in with_clause.aggregations:
            parts.append(f"{agg.function}({agg.field}) AS {agg.alias}")
    return "WITH " + ", ".join(parts)

def build_return_clause(return_clause: ReturnClause) -> str:
    """Builds the RETURN clause."""
    return_str = "RETURN"
    if return_clause.distinct:
        return_str += " DISTINCT"
    return_str += " " + ", ".join(return_clause.fields)
    return return_str

def build_orderby_clause(orderby_clause: OrderByClause) -> str:
    """Builds the ORDER BY clause."""
    return f"ORDER BY {orderby_clause.field} {orderby_clause.direction}"

def build_limit_clause(limit: int) -> str:
    """Builds the LIMIT clause."""
    return f"LIMIT {limit}"

# =========================
# Main Conversion Function
# =========================

def convert_json_to_cypher(json_data: Dict[str, Any], optimize: bool = True) -> str:
    """Converts a JSON Cypher spec to a Cypher query string."""
    try:
        cypher_data = CypherQuery.model_validate(json_data)
        validate_aliases(cypher_data.nodes, cypher_data.relationships)
        if cypher_data.whereClause:
            validate_conditions(cypher_data.whereClause.conditions)
        validate_field_syntax(cypher_data.returnClause.fields)

        query_parts: List[str] = []

        if optimize:
            match_clause = build_advanced_match_clause(
                cypher_data.nodes,
                cypher_data.relationships,
                cypher_data.returnClause,
                cypher_data.whereClause
            )
        else:
            match_clause = _build_single_match_clause(cypher_data.nodes, cypher_data.relationships)

        if match_clause:
            query_parts.append(match_clause)

        if cypher_data.whereClause:
            query_parts.append(build_where_clause(cypher_data.whereClause))

        if cypher_data.with_:
            query_parts.append(build_with_clause(cypher_data.with_))
        if cypher_data.withClause:
            query_parts.append(build_where_clause(cypher_data.withClause))

        query_parts.append(build_return_clause(cypher_data.returnClause))

        if cypher_data.orderBy:
            query_parts.append(build_orderby_clause(cypher_data.orderBy))

        if cypher_data.limit is not None:
            query_parts.append(build_limit_clause(cypher_data.limit))

        return "\n".join(query_parts)

    except Exception as e:
        return f"❌ Error: {e}"

# =========================
# Batch Processing Function
# =========================

def process_all_json_files(directory: str = "automated_cypher_to_JSON", output_file: str = "final_optimized_cypher_queries.txt") -> None:
    print("🚀 FINAL OPTIMIZED CYPHER CONVERTER")
    print(" Single-MATCH chaining with anchored OPTIONAL handling")
    print("=" * 80)

    if not os.path.exists(directory):
        print(f"❌ Directory {directory} not found!")
        return

    json_files = [f for f in os.listdir(directory) if f.endswith('.json')]
    if not json_files:
        print(f"❌ No JSON files found in {directory}")
        return

    results = []
    success_count = 0

    for json_file in sorted(json_files):
        file_path = os.path.join(directory, json_file)
        print(f"\n📁 Processing: {json_file}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)

            cypher_query = convert_json_to_cypher(json_data, optimize=True)
            status = "✅ SUCCESS" if not cypher_query.startswith("❌") else "❌ FAILED"

            if status == "✅ SUCCESS":
                success_count += 1
                first_line = cypher_query.split('\n')
                print(f" {status}")
                print(f" Preview: {first_line}")
                if "<-[:" in cypher_query:
                    print(" 🎯 OPTIMAL: Contains backward traversal")
                else:
                    print(" 📋 STANDARD: Forward-only chain")
            else:
                print(f" {status}: {cypher_query}")

            results.append({
                'file': json_file,
                'status': status,
                'query': cypher_query
            })

        except Exception as e:
            error_msg = f"❌ Error: {e}"
            print(f" {error_msg}")
            results.append({
                'file': json_file,
                'status': "❌ FAILED",
                'query': error_msg
            })

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("FINAL OPTIMIZED CYPHER QUERIES\n")
        f.write("Generated with single-MATCH chaining optimization and anchored OPTIONALs\n")
        f.write("=" * 80 + "\n\n")
        for result in results:
            f.write(f"FILE: {result['file']}\n")
            f.write(f"STATUS: {result['status']}\n")
            f.write(f"QUERY:\n{result['query']}\n")
            f.write("-" * 60 + "\n\n")

    print(f"\n📊 FINAL SUMMARY:")
    print(f" Total files: {len(json_files)}")
    print(f" Successful: {success_count}")
    print(f" Failed: {len(json_files) - success_count}")
    print(f" Success rate: {(success_count/len(json_files)*100):.1f}%")
    print(f"\n💾 Results saved to: {output_file}")

# =========================
# Entry Point
# =========================

if __name__ == "__main__":
    process_all_json_files()

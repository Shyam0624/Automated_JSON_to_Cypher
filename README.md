# 🚀 Automated JSON to Cypher Converter

[![Python](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 📚 Table of Contents

- [📝 Description](#-description)
- [✨ Features](#-features)
- [⚙️ Installation](#️-installation)
- [▶️ Usage](#-usage)
- [📥 Input Format](#-input-format)
- [📤 Output](#-output)
- [🗂️ Directory Structure](#-directory-structure)
- [🛡️ License](#-license)
- [🙏 Credits & Acknowledgements](#-credits--acknowledgements)
- [🧪 Tests](#-tests)

---

## 📝 Description

Automated JSON to Cypher Converter is a Python application that reads graph query specifications in JSON format and generates optimized Cypher queries for Neo4j.  
It supports advanced features like chained MATCH and OPTIONAL MATCH clauses, property filtering, aggregation, and robust error handling for invalid input.

---

## ✨ Features

- 🔄 **JSON-to-Cypher Translation:** Converts structured JSON query specs into valid Cypher queries.
- ⚡ **Optimized Pattern Generation:** Produces single-chain `MATCH` and anchored `OPTIONAL MATCH` clauses for efficient querying.
- 🛡️ **Validation:** Checks for alias correctness, field syntax (including backticks for spaces), and reports errors with clear messages.
- 📂 **Batch Processing:** Processes all JSON files in a directory and outputs results to a summary file.
- 🧩 **Customizable Output:** Supports `WHERE`, `WITH`, `RETURN`, `ORDER BY`, and `LIMIT` clauses.
- 🏗️ **Extensible:** Easily adaptable for new node/relationship types or query patterns.
- 🚨 **Error Reporting:** Logs and displays errors for invalid JSON or Cypher specs.

---

## ⚙️ Installation

### Prerequisites

- Python 3.8 or higher

### Clone the Repository

git clone https://github.com/Shyam0624/automated-json-to-cypher.git
cd automated-json-to-cypher

### Install Dependencies

pip install -r requirements.txt

---

## ▶️ Usage

### 1. Prepare Your JSON Queries

- Place your JSON query files in the input_json_queries/ directory.
- Each file should follow the Input Format described below.

### 2. Run the Converter

python JSONtoCypher_production.py

- The script will process all .json files in input_json_queries/ (or the specified directory).
- Output Cypher queries and statuses are saved to final_optimized_cypher_queries.txt.

### 3. Example Output

📁 Processing: test_output1.json
 ✅ SUCCESS
 Preview: ['MATCH (j:Job)-[:REQUIRES]->(s:Skill)<-[:HAS_SKILL]-(r:Resume)<-[:HAS_RESUME]-(c:Candidate)', ...]

---

## 📥 Input Format

Each JSON file should define nodes, relationships, and query clauses.  
Example:

{
  "nodes": [
    { "label": "Candidate", "alias": "c" },
    { "label": "Resume", "alias": "r" },
    { "label": "Job", "alias": "j" }
  ],
  "relationships": [
    { "node1": "c", "node2": "r", "type": "HAS_RESUME" },
    { "node1": "r", "node2": "j", "type": "SUBMITTED_FOR", "optional": true }
  ],
  "whereClause": {
    "type": "AND",
    "conditions": [
      { "field": "c.`Email`", "operator": "=", "value": "alice@example.com" }
    ]
  },
  "return": {
    "fields": [ "c.`Email`", "j.`Job Title`" ],
    "distinct": true
  }
}

Key fields:
- nodes: List of graph nodes with label and alias.
- relationships: List of relationships, with optional "optional": true for OPTIONAL MATCH.
- whereClause: (Optional) Filtering conditions.
- return: Fields to return, with optional distinct.

---

## 📤 Output

- Cypher queries are printed to the terminal and saved in final_optimized_cypher_queries.txt.
- Each entry includes the file name, status, and the generated query or error message.

---

## 🗂️ Directory Structure

Automated-JSON-to-Cypher/
│
├── JSONtoCypher_production.py
├── input_json_queries/
│   ├── test_output1.json
│   ├── test_output2.json
│   └── ... (your test JSONs)
├── final_optimized_cypher_queries.txt
├── README.md
├── requirements.txt
└── .gitignore

---

## 🛡️ License

This project is licensed under the MIT License.

---

## 🙏 Credits & Acknowledgements

- Neo4j for the Cypher query language.
- Pydantic for data validation.
- Python community for libraries and support.

---

## 🌟 Keep Querying
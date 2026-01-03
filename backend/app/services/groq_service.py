"""
Groq LLM Service
================
Natural language to SQL conversion using Groq's API.
"""

import logging
from groq import Groq
from typing import Dict, Any, Optional
from app.config import settings

logger = logging.getLogger(__name__)


class GroqService:
    """Service for natural language to SQL conversion using Groq LLM."""
    
    def __init__(self):
        """Initialize Groq client."""
        if not settings.groq_api_key or settings.groq_api_key == "your_groq_api_key_here":
            logger.warning("⚠️  GROQ_API_KEY not configured - LLM features disabled")
            self.client = None
        else:
            try:
                self.client = Groq(api_key=settings.groq_api_key)
                logger.info(f"✅ Groq client initialized with model: {settings.groq_model}")
            except Exception as e:
                logger.error(f"❌ Failed to initialize Groq client: {str(e)}")
                self.client = None
    
    def is_available(self) -> bool:
        """Check if Groq service is available."""
        return self.client is not None
    
    async def natural_language_to_sql(
        self,
        instruction: str,
        schema: Dict[str, Any],
        table_format: str = "parquet"
    ) -> Dict[str, Any]:
        """
        Convert natural language instruction to SQL query.
        
        Args:
            instruction: Natural language instruction (e.g., "delete rows where age > 30")
            schema: Table schema with columns information
            table_format: Type of table (parquet, iceberg, delta, hudi)
            
        Returns:
            Dict with:
                - sql: Generated SQL query
                - explanation: Plain English explanation of what the SQL does
                - operation_type: Type of operation (SELECT, INSERT, UPDATE, DELETE)
                - affected_columns: List of columns affected
                - requires_backup: Whether backup is recommended
        """
        if not self.is_available():
            raise ValueError("Groq API key not configured. Set GROQ_API_KEY in .env file")
        
        # Build schema description for the prompt
        schema_desc = self._format_schema_for_prompt(schema)
        
        # Create prompt for Groq
        prompt = self._build_sql_generation_prompt(
            instruction=instruction,
            schema_description=schema_desc,
            table_format=table_format
        )
        
        try:
            logger.info(f"🤖 Converting to SQL: '{instruction}'")
            
            # Call Groq API
            response = self.client.chat.completions.create(
                model=settings.groq_model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert SQL engineer. Convert natural language instructions to DuckDB SQL queries. Always respond in valid JSON format."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.1,  # Low temperature for consistent SQL generation
                max_tokens=1000
            )
            
            # Parse response
            result = self._parse_llm_response(response.choices[0].message.content)
            
            logger.info(f"✅ Generated SQL: {result['sql']}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Error generating SQL: {str(e)}")
            raise ValueError(f"Failed to generate SQL: {str(e)}")
    
    def _format_schema_for_prompt(self, schema: Dict[str, Any]) -> str:
        """Format schema information for the LLM prompt."""
        columns = schema.get("columns", [])
        
        schema_lines = ["Table Schema:"]
        schema_lines.append("Columns:")
        
        for col in columns:
            nullable = "NULL" if col.get("nullable", True) else "NOT NULL"
            schema_lines.append(f"  - {col['name']}: {col['type']} ({nullable})")
        
        if "partition_columns" in schema and schema["partition_columns"]:
            schema_lines.append(f"\nPartitioned by: {', '.join(schema['partition_columns'])}")
        
        if "statistics" in schema and schema["statistics"].get("num_rows"):
            schema_lines.append(f"\nApproximate rows: {schema['statistics']['num_rows']:,}")
        
        return "\n".join(schema_lines)
    
    def _build_sql_generation_prompt(
        self,
        instruction: str,
        schema_description: str,
        table_format: str
    ) -> str:
        """Build the prompt for SQL generation."""
        return f"""Given this {table_format.upper()} table:

{schema_description}

User instruction: "{instruction}"

Generate a DuckDB SQL query to accomplish this task. The table is referenced as 'data_table' in your SQL.

IMPORTANT RULES:
1. Use DuckDB SQL syntax
2. Table name is 'data_table'
3. For SELECT queries, include WHERE/LIMIT to avoid reading too much data
4. For DELETE/UPDATE, always include a WHERE clause (no unbounded operations)
5. Be conservative - prefer smaller changes over large ones

Respond in this EXACT JSON format:
{{
    "sql": "your SQL query here",
    "explanation": "plain English explanation of what this does",
    "operation_type": "SELECT|INSERT|UPDATE|DELETE",
    "affected_columns": ["list", "of", "columns"],
    "requires_backup": true/false,
    "estimated_impact": "description of impact"
}}

Example responses:

For "show me users from California":
{{
    "sql": "SELECT * FROM data_table WHERE state = 'California' LIMIT 1000",
    "explanation": "Retrieves up to 1000 rows where state equals California",
    "operation_type": "SELECT",
    "affected_columns": ["state"],
    "requires_backup": false,
    "estimated_impact": "Read-only query, no data modification"
}}

For "delete users older than 30":
{{
    "sql": "DELETE FROM data_table WHERE age > 30",
    "explanation": "Removes all rows where the age column is greater than 30",
    "operation_type": "DELETE",
    "affected_columns": ["age"],
    "requires_backup": true,
    "estimated_impact": "Permanently deletes rows matching the condition"
}}

For "update California to CA":
{{
    "sql": "UPDATE data_table SET state = 'CA' WHERE state = 'California'",
    "explanation": "Changes state value from 'California' to 'CA' for matching rows",
    "operation_type": "UPDATE",
    "affected_columns": ["state"],
    "requires_backup": true,
    "estimated_impact": "Modifies existing row values"
}}

Now generate SQL for: "{instruction}"
"""
    
    def _parse_llm_response(self, response_text: str) -> Dict[str, Any]:
        """Parse LLM response and extract SQL information."""
        import json
        
        try:
            # Try to parse as JSON
            # Sometimes LLM wraps in markdown code blocks, so strip those
            cleaned = response_text.strip()
            if cleaned.startswith("```json"):
                cleaned = cleaned[7:]
            if cleaned.startswith("```"):
                cleaned = cleaned[3:]
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]
            cleaned = cleaned.strip()
            
            result = json.loads(cleaned)
            
            # Validate required fields
            required_fields = ["sql", "explanation", "operation_type"]
            for field in required_fields:
                if field not in result:
                    raise ValueError(f"Missing required field: {field}")
            
            # Set defaults for optional fields
            result.setdefault("affected_columns", [])
            result.setdefault("requires_backup", True)  # Safe default
            result.setdefault("estimated_impact", "Unknown impact")
            
            return result
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response as JSON: {response_text}")
            raise ValueError(f"Invalid JSON response from LLM: {str(e)}")
    
    async def validate_sql_safety(self, sql: str) -> Dict[str, Any]:
        """
        Validate SQL query for safety concerns.
        
        Args:
            sql: SQL query to validate
            
        Returns:
            Dict with:
                - is_safe: bool
                - warnings: list of warning messages
                - risk_level: low/medium/high
        """
        warnings = []
        risk_level = "low"
        
        sql_upper = sql.upper()
        
        # Check for dangerous patterns
        if "DELETE FROM" in sql_upper and "WHERE" not in sql_upper:
            warnings.append("⚠️  DELETE without WHERE clause - will delete ALL rows!")
            risk_level = "high"
        
        if "UPDATE" in sql_upper and "WHERE" not in sql_upper:
            warnings.append("⚠️  UPDATE without WHERE clause - will update ALL rows!")
            risk_level = "high"
        
        if "DROP TABLE" in sql_upper or "DROP DATABASE" in sql_upper:
            warnings.append("🚫 DROP operations are not allowed!")
            risk_level = "high"
        
        if "TRUNCATE" in sql_upper:
            warnings.append("⚠️  TRUNCATE will delete all data!")
            risk_level = "high"
        
        # Check for multiple statements (SQL injection risk)
        if sql.count(";") > 1:
            warnings.append("⚠️  Multiple SQL statements detected - only single statements allowed")
            risk_level = "high"
        
        is_safe = risk_level != "high" or len(warnings) == 0
        
        return {
            "is_safe": is_safe,
            "warnings": warnings,
            "risk_level": risk_level
        }


# Global service instance
groq_service = GroqService()

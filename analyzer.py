"""
Data Engineering PySpark Code Quality Analyzer

This tool analyzes PySpark code against Data Engineering Coding Guidelines and provides:
1. Quality scoring (1-5 scale)
2. Code smell identification with criticality
3. Specific fixing suggestions
"""

import re
import ast
import json
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass
from enum import Enum


class Criticality(Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class Priority(Enum):
    P1 = "P1 - Fix Immediately"
    P2 = "P2 - Fix in Current Sprint"
    P3 = "P3 - Fix in Next Sprint"
    P4 = "P4 - Fix When Convenient"


@dataclass
class CodeSmell:
    category: str
    description: str
    line_number: int
    code_snippet: str
    criticality: Criticality
    priority: Priority
    suggestion: str
    guideline_section: str


class DataEngineeringCodeAnalyzer:
    def __init__(self):
        self.code_smells = []
        self.quality_scores = {
            'structure_organization': 0,
            'function_design': 0,
            'error_handling': 0,
            'logging': 0,
            'performance': 0,
            'delta_lake_usage': 0,
            'naming_conventions': 0,
            'documentation': 0
        }
        
    def analyze_code(self, code: str, filename: str = "unknown") -> Dict[str, Any]:
        """Main analysis method"""
        lines = code.split('\n')
        
        # Reset for new analysis
        self.code_smells = []
        self.quality_scores = {key: 0 for key in self.quality_scores}
        
        # Run all analysis checks
        self._check_structure_organization(lines)
        self._check_function_design(lines)
        self._check_error_handling(lines)
        self._check_logging_practices(lines)
        self._check_performance_issues(lines)
        self._check_delta_lake_usage(lines)
        self._check_naming_conventions(lines)
        self._check_documentation(lines)
        
        # Calculate overall score
        overall_score = self._calculate_overall_score()
        
        return {
            'filename': filename,
            'overall_score': overall_score,
            'category_scores': self.quality_scores,
            'code_smells': [self._smell_to_dict(smell) for smell in self.code_smells],
            'total_smells': len(self.code_smells),
            'critical_smells': len([s for s in self.code_smells if s.criticality == Criticality.CRITICAL]),
            'high_priority_smells': len([s for s in self.code_smells if s.priority == Priority.P1])
        }
    
    def _check_structure_organization(self, lines: List[str]):
        """Check code structure and organization (Section 2.1)"""
        score = 5
        
        # Check for proper notebook structure
        has_imports = False
        has_config = False
        has_main_function = False
        has_execution_control = False
        
        for i, line in enumerate(lines):
            line_clean = line.strip()
            
            # Check for imports
            if line_clean.startswith('from pyspark') or line_clean.startswith('import'):
                has_imports = True
            
            # Check for configuration loading
            if any(keyword in line_clean for keyword in ['REGION =', 'WORKSPACE_NAME =', 'batch_date =']):
                has_config = True
            
            # Check for main function
            if 'def execute_main_pipeline' in line_clean:
                has_main_function = True
            
            # Check for execution control
            if '%run' in line_clean and ('Dimensions Delta' in line_clean or 'Facts Delta' in line_clean):
                has_execution_control = True
            
            # Check for monolithic code (large functions)
            if line_clean.startswith('def ') and i < len(lines) - 50:
                func_lines = 0
                for j in range(i + 1, min(i + 200, len(lines))):
                    if lines[j].strip() and not lines[j].startswith(' ') and not lines[j].startswith('\t'):
                        break
                    func_lines += 1
                
                if func_lines > 100:
                    self.code_smells.append(CodeSmell(
                        category="Structure & Organization",
                        description="Function is too large (>100 lines)",
                        line_number=i + 1,
                        code_snippet=line_clean,
                        criticality=Criticality.MEDIUM,
                        priority=Priority.P2,
                        suggestion="Split large function into smaller, focused functions. Each function should handle a single responsibility.",
                        guideline_section="2.1 - Modularity"
                    ))
                    score -= 1
        
        # Evaluate structure completeness
        if not has_imports:
            self.code_smells.append(CodeSmell(
                category="Structure & Organization",
                description="Missing proper import structure",
                line_number=1,
                code_snippet="Missing imports section",
                criticality=Criticality.HIGH,
                priority=Priority.P1,
                suggestion="Add proper imports section at the beginning: from pyspark.sql import functions as F, etc.",
                guideline_section="2.1 - Notebook Structure"
            ))
            score -= 2
        
        if not has_config:
            self.code_smells.append(CodeSmell(
                category="Structure & Organization",
                description="Missing configuration loading section",
                line_number=1,
                code_snippet="Missing configuration",
                criticality=Criticality.HIGH,
                priority=Priority.P1,
                suggestion="Add configuration section with REGION, workspace names, and paths as per template",
                guideline_section="2.1 - Notebook Structure"
            ))
            score -= 2
        
        if not has_main_function:
            self.code_smells.append(CodeSmell(
                category="Structure & Organization",
                description="Missing execute_main_pipeline() function",
                line_number=1,
                code_snippet="Missing main function",
                criticality=Criticality.CRITICAL,
                priority=Priority.P1,
                suggestion="Encapsulate all transformation logic in execute_main_pipeline() function",
                guideline_section="2.1 - Function-Based Design"
            ))
            score -= 3
        
        self.quality_scores['structure_organization'] = max(0, score)
    
    def _check_function_design(self, lines: List[str]):
        """Check function-based design practices (Section 2.1)"""
        score = 5
        
        inline_transformations = 0
        functions_with_docstrings = 0
        total_functions = 0
        
        for i, line in enumerate(lines):
            line_clean = line.strip()
            
            # Check for inline transformations (not in functions)
            if any(method in line_clean for method in ['.withColumn(', '.select(', '.filter(', '.groupBy(']):
                if not self._is_inside_function(lines, i):
                    inline_transformations += 1
                    if inline_transformations == 1:  # Report only first occurrence
                        self.code_smells.append(CodeSmell(
                            category="Function Design",
                            description="Transformation logic written inline instead of in functions",
                            line_number=i + 1,
                            code_snippet=line_clean,
                            criticality=Criticality.HIGH,
                            priority=Priority.P1,
                            suggestion="Encapsulate transformation logic in functions with meaningful names and docstrings",
                            guideline_section="2.1 - Function-Based Design"
                        ))
            
            # Check function definitions and docstrings
            if line_clean.startswith('def '):
                total_functions += 1
                # Check for docstring in next few lines
                has_docstring = False
                for j in range(i + 1, min(i + 5, len(lines))):
                    if '"""' in lines[j] or "'''" in lines[j]:
                        has_docstring = True
                        functions_with_docstrings += 1
                        break
                
                if not has_docstring:
                    self.code_smells.append(CodeSmell(
                        category="Function Design",
                        description="Function missing docstring",
                        line_number=i + 1,
                        code_snippet=line_clean,
                        criticality=Criticality.MEDIUM,
                        priority=Priority.P3,
                        suggestion="Add comprehensive docstring with Args, Returns, and description as per example in guidelines",
                        guideline_section="2.1 - Function-Based Design"
                    ))
        
        # Scoring adjustments
        if inline_transformations > 5:
            score -= 3
        elif inline_transformations > 2:
            score -= 2
        elif inline_transformations > 0:
            score -= 1
        
        if total_functions > 0:
            docstring_ratio = functions_with_docstrings / total_functions
            if docstring_ratio < 0.5:
                score -= 1
        
        self.quality_scores['function_design'] = max(0, score)
    
    def _check_error_handling(self, lines: List[str]):
        """Check error handling practices (Section 2.4)"""
        score = 5
        
        has_try_except = False
        has_status_management = False
        has_retry_logic = False
        
        for i, line in enumerate(lines):
            line_clean = line.strip()
            
            # Check for try-except blocks
            if line_clean.startswith('try:'):
                has_try_except = True
                
                # Check if corresponding except has proper logging
                except_found = False
                proper_logging = False
                for j in range(i + 1, min(i + 20, len(lines))):
                    if lines[j].strip().startswith('except'):
                        except_found = True
                    if except_found and 'logging.' in lines[j]:
                        proper_logging = True
                        break
                
                if except_found and not proper_logging:
                    self.code_smells.append(CodeSmell(
                        category="Error Handling",
                        description="Exception caught but not properly logged",
                        line_number=j + 1,
                        code_snippet=lines[j].strip(),
                        criticality=Criticality.HIGH,
                        priority=Priority.P1,
                        suggestion="Add logging.error() with context (table name, batch date, etc.) and exc_info=True",
                        guideline_section="2.4 - Log Context-Rich Errors"
                    ))
            
            # Check for status management
            if any(status in line_clean for status in ['IN PROGRESS', 'COMPLETED', 'FAILED']):
                has_status_management = True
            
            # Check for retry decorators
            if '@retry' in line_clean:
                has_retry_logic = True
            
            # Check for risky operations without error handling
            if any(risky in line_clean for risky in ['.read.', '.write.', 'spark.sql']):
                if not self._is_in_try_block(lines, i):
                    self.code_smells.append(CodeSmell(
                        category="Error Handling",
                        description="Risky I/O operation not wrapped in try-except",
                        line_number=i + 1,
                        code_snippet=line_clean,
                        criticality=Criticality.HIGH,
                        priority=Priority.P1,
                        suggestion="Wrap I/O operations in try-except blocks with proper error logging",
                        guideline_section="2.4 - Try-Except Blocks"
                    ))
        
        # Scoring
        if not has_try_except:
            score -= 3
        if not has_status_management:
            score -= 1
        if not has_retry_logic:
            score -= 1
        
        self.quality_scores['error_handling'] = max(0, score)
    
    def _check_logging_practices(self, lines: List[str]):
        """Check logging practices (Section 2.3)"""
        score = 5
        
        has_logging_import = False
        has_logging_config = False
        uses_print_instead_logging = False
        has_structured_logging = False
        
        for i, line in enumerate(lines):
            line_clean = line.strip()
            
            # Check for logging import and configuration
            if 'import logging' in line_clean:
                has_logging_import = True
            
            if 'logging.basicConfig' in line_clean:
                has_logging_config = True
            
            # Check for print statements (should use logging instead)
            if line_clean.startswith('print(') and 'debug' not in line_clean.lower():
                uses_print_instead_logging = True
                self.code_smells.append(CodeSmell(
                    category="Logging",
                    description="Using print() instead of logging",
                    line_number=i + 1,
                    code_snippet=line_clean,
                    criticality=Criticality.MEDIUM,
                    priority=Priority.P2,
                    suggestion="Replace print() with appropriate logging level (logging.info, logging.warning, etc.)",
                    guideline_section="2.3 - Logging Standards"
                ))
            
            # Check for structured logging
            if any(level in line_clean for level in ['logging.info', 'logging.warning', 'logging.error']):
                has_structured_logging = True
        
        # Scoring
        if not has_logging_import:
            score -= 2
        if not has_logging_config:
            score -= 1
        if uses_print_instead_logging:
            score -= 1
        if not has_structured_logging:
            score -= 1
        
        self.quality_scores['logging'] = max(0, score)
    
    def _check_performance_issues(self, lines: List[str]):
        """Check performance issues (Section 2.5)"""
        score = 5
        
        for i, line in enumerate(lines):
            line_clean = line.strip()
            
            # Check for performance anti-patterns
            if '.collect()' in line_clean:
                self.code_smells.append(CodeSmell(
                    category="Performance",
                    description="Using .collect() which brings all data to driver",
                    line_number=i + 1,
                    code_snippet=line_clean,
                    criticality=Criticality.HIGH,
                    priority=Priority.P1,
                    suggestion="Avoid .collect() on large datasets. Use .limit().toPandas() for debugging or rethink the logic",
                    guideline_section="2.5 - Avoid collecting large DataFrames"
                ))
                score -= 2
            
            if '.count()' in line_clean and 'logging' not in line_clean:
                self.code_smells.append(CodeSmell(
                    category="Performance",
                    description="Unnecessary .count() operation",
                    line_number=i + 1,
                    code_snippet=line_clean,
                    criticality=Criticality.MEDIUM,
                    priority=Priority.P2,
                    suggestion="Avoid .count() unless essential for business logic. Use for logging only when necessary",
                    guideline_section="2.5 - Minimize actions"
                ))
                score -= 1
            
            if '.show()' in line_clean:
                self.code_smells.append(CodeSmell(
                    category="Performance",
                    description="Using .show() in production code",
                    line_number=i + 1,
                    code_snippet=line_clean,
                    criticality=Criticality.MEDIUM,
                    priority=Priority.P2,
                    suggestion="Remove .show() from production code. Use .limit(5).toPandas() for debugging in non-prod",
                    guideline_section="2.5 - Avoid unnecessary actions"
                ))
                score -= 1
            
            # Check for missing broadcast hints
            if '.join(' in line_clean and 'broadcast(' not in line_clean:
                self.code_smells.append(CodeSmell(
                    category="Performance",
                    description="Join without broadcast hint for potentially small table",
                    line_number=i + 1,
                    code_snippet=line_clean,
                    criticality=Criticality.LOW,
                    priority=Priority.P4,
                    suggestion="Consider using broadcast() for small tables in joins to avoid shuffle",
                    guideline_section="2.5 - Use broadcast joins wisely"
                ))
            
            # Check for missing cache unpersist
            if '.cache()' in line_clean:
                # Look for corresponding unpersist
                unpersist_found = False
                for j in range(i + 1, min(i + 50, len(lines))):
                    if '.unpersist()' in lines[j]:
                        unpersist_found = True
                        break
                
                if not unpersist_found:
                    self.code_smells.append(CodeSmell(
                        category="Performance",
                        description="DataFrame cached but not unpersisted",
                        line_number=i + 1,
                        code_snippet=line_clean,
                        criticality=Criticality.MEDIUM,
                        priority=Priority.P2,
                        suggestion="Add .unpersist() after DataFrame is no longer needed to free memory",
                        guideline_section="2.5 - Always unpersist cached data"
                    ))
                    score -= 1
        
        self.quality_scores['performance'] = max(0, score)
    
    def _check_delta_lake_usage(self, lines: List[str]):
        """Check Delta Lake usage (Section 3)"""
        score = 5
        
        has_delta_format = False
        has_merge_logic = False
        uses_parquet = False
        
        for i, line in enumerate(lines):
            line_clean = line.strip()
            
            # Check for Delta format usage
            if '.format("delta")' in line_clean or 'DeltaTable' in line_clean:
                has_delta_format = True
            
            # Check for merge operations
            if '.merge(' in line_clean:
                has_merge_logic = True
            
            # Check for Parquet usage (should be Delta)
            if '.format("parquet")' in line_clean:
                uses_parquet = True
                self.code_smells.append(CodeSmell(
                    category="Delta Lake",
                    description="Using Parquet format instead of recommended Delta format",
                    line_number=i + 1,
                    code_snippet=line_clean,
                    criticality=Criticality.CRITICAL,
                    priority=Priority.P1,
                    suggestion="Replace with .format('delta') as per guidelines - Delta is recommended for all data",
                    guideline_section="3.1 - Recommended Use Cases"
                ))
                score -= 3
            
            # Check for proper SCD handling
            if 'SCD' in line_clean or 'dim_type' in line_clean:
                if 'TYPE-1' in line_clean:
                    # Should have whenMatchedUpdate
                    merge_found = False
                    for j in range(max(0, i - 10), min(i + 10, len(lines))):
                        if 'whenMatchedUpdate' in lines[j]:
                            merge_found = True
                            break
                    if not merge_found:
                        self.code_smells.append(CodeSmell(
                            category="Delta Lake",
                            description="SCD Type-1 declared but no proper merge logic found",
                            line_number=i + 1,
                            code_snippet=line_clean,
                            criticality=Criticality.HIGH,
                            priority=Priority.P1,
                            suggestion="Implement proper SCD Type-1 merge with whenMatchedUpdate and whenNotMatchedInsert",
                            guideline_section="3.2 - SCD Type 1 Implementation"
                        ))
        
        if not has_delta_format:
            score -= 2
        
        self.quality_scores['delta_lake_usage'] = max(0, score)
    
    def _check_naming_conventions(self, lines: List[str]):
        """Check naming conventions (Section 2.2)"""
        score = 5
        
        for i, line in enumerate(lines):
            line_clean = line.strip()
            
            # Check variable naming (should be snake_case)
            if '=' in line_clean and not line_clean.startswith('#'):
                var_match = re.match(r'(\w+)\s*=', line_clean)
                if var_match:
                    var_name = var_match.group(1)
                    if var_name.isupper():  # Constants are OK
                        continue
                    if not re.match(r'^[a-z][a-z0-9_]*$', var_name) and var_name not in ['df', 'i', 'j']:
                        self.code_smells.append(CodeSmell(
                            category="Naming Conventions",
                            description=f"Variable '{var_name}' doesn't follow snake_case convention",
                            line_number=i + 1,
                            code_snippet=line_clean,
                            criticality=Criticality.LOW,
                            priority=Priority.P4,
                            suggestion="Use snake_case for variable names (e.g., user_data, batch_date)",
                            guideline_section="2.2 - Naming Conventions"
                        ))
            
            # Check function naming
            if line_clean.startswith('def '):
                func_match = re.match(r'def\s+(\w+)', line_clean)
                if func_match:
                    func_name = func_match.group(1)
                    if not re.match(r'^[a-z][a-z0-9_]*$', func_name):
                        self.code_smells.append(CodeSmell(
                            category="Naming Conventions",
                            description=f"Function '{func_name}' doesn't follow snake_case convention",
                            line_number=i + 1,
                            code_snippet=line_clean,
                            criticality=Criticality.LOW,
                            priority=Priority.P4,
                            suggestion="Use snake_case for function names (e.g., execute_main_pipeline, generate_hash_value)",
                            guideline_section="2.2 - Naming Conventions"
                        ))
        
        self.quality_scores['naming_conventions'] = max(1, score)  # Minor impact on overall score
    
    def _check_documentation(self, lines: List[str]):
        """Check documentation quality"""
        score = 5
        
        has_header_comment = False
        comment_ratio = 0
        total_lines = len([l for l in lines if l.strip()])
        comment_lines = len([l for l in lines if l.strip().startswith('#')])
        
        if total_lines > 0:
            comment_ratio = comment_lines / total_lines
        
        # Check for header documentation
        for i in range(min(10, len(lines))):
            if lines[i].strip().startswith('#') and ('purpose' in lines[i].lower() or 'description' in lines[i].lower()):
                has_header_comment = True
                break
        
        if not has_header_comment:
            self.code_smells.append(CodeSmell(
                category="Documentation",
                description="Missing header documentation explaining notebook purpose",
                line_number=1,
                code_snippet="# Missing header",
                criticality=Criticality.LOW,
                priority=Priority.P4,
                suggestion="Add header comment explaining the notebook's purpose, data sources, and outputs",
                guideline_section="Documentation Best Practices"
            ))
            score -= 1
        
        if comment_ratio < 0.05:  # Less than 5% comments
            self.code_smells.append(CodeSmell(
                category="Documentation",
                description="Insufficient inline documentation",
                line_number=1,
                code_snippet="Low comment ratio",
                criticality=Criticality.LOW,
                priority=Priority.P4,
                suggestion="Add more inline comments explaining complex logic and business rules",
                guideline_section="Documentation Best Practices"
            ))
            score -= 1
        
        self.quality_scores['documentation'] = max(0, score)
    
    def _calculate_overall_score(self) -> float:
        """Calculate weighted overall score"""
        weights = {
            'structure_organization': 0.20,
            'function_design': 0.15,
            'error_handling': 0.20,
            'logging': 0.10,
            'performance': 0.15,
            'delta_lake_usage': 0.15,
            'naming_conventions': 0.03,
            'documentation': 0.02
        }
        
        weighted_score = sum(self.quality_scores[category] * weight 
                           for category, weight in weights.items())
        
        return round(weighted_score, 1)
    
    def _is_inside_function(self, lines: List[str], line_index: int) -> bool:
        """Check if a line is inside a function definition"""
        for i in range(line_index - 1, -1, -1):
            line = lines[i].strip()
            if line.startswith('def '):
                return True
            if line and not line.startswith(' ') and not line.startswith('\t') and line != '':
                if not line.startswith('#') and not line.startswith('def '):
                    return False
        return False
    
    def _is_in_try_block(self, lines: List[str], line_index: int) -> bool:
        """Check if a line is inside a try block"""
        for i in range(line_index - 1, -1, -1):
            line = lines[i].strip()
            if line.startswith('try:'):
                return True
            if line.startswith('except') or (line and not line.startswith(' ') and not line.startswith('\t')):
                return False
        return False
    
    def _smell_to_dict(self, smell: CodeSmell) -> Dict[str, Any]:
        """Convert CodeSmell to dictionary for JSON serialization"""
        return {
            'category': smell.category,
            'description': smell.description,
            'line_number': smell.line_number,
            'code_snippet': smell.code_snippet,
            'criticality': smell.criticality.value,
            'priority': smell.priority.value,
            'suggestion': smell.suggestion,
            'guideline_section': smell.guideline_section
        }
    
    def generate_report(self, analysis_result: Dict[str, Any]) -> str:
        """Generate a human-readable report"""
        report = f"""
DATA ENGINEERING PYSPARK CODE QUALITY ANALYSIS REPORT
=====================================================

File: {analysis_result['filename']}
Overall Quality Score: {analysis_result['overall_score']}/5.0

SCORE BREAKDOWN:
"""
        
        for category, score in analysis_result['category_scores'].items():
            category_name = category.replace('_', ' ').title()
            report += f"  {category_name}: {score}/5\n"
        
        report += f"""
SUMMARY:
  Total Code Smells: {analysis_result['total_smells']}
  Critical Issues: {analysis_result['critical_smells']}
  High Priority Items: {analysis_result['high_priority_smells']}

CODE SMELLS DETECTED:
=====================
"""
        
        # Group smells by priority
        smells_by_priority = {}
        for smell in analysis_result['code_smells']:
            priority = smell['priority']
            if priority not in smells_by_priority:
                smells_by_priority[priority] = []
            smells_by_priority[priority].append(smell)
        
        # Report by priority
        for priority in ['P1 - Fix Immediately', 'P2 - Fix in Current Sprint', 
                        'P3 - Fix in Next Sprint', 'P4 - Fix When Convenient']:
            if priority in smells_by_priority:
                report += f"\n{priority}:\n" + "-" * (len(priority) + 1) + "\n"
                for smell in smells_by_priority[priority]:
                    report += f"""
Category: {smell['category']}
Line {smell['line_number']}: {smell['description']}
Code: {smell['code_snippet']}
Criticality: {smell['criticality']}
Guideline: {smell['guideline_section']}
Suggestion: {smell['suggestion']}
"""
        
        return report


def analyze_file_from_path(file_path: str) -> Dict[str, Any]:
    """
    Analyze a PySpark code file from the given path
    
    Args:
        file_path (str): Path to the Python/PySpark file to analyze
        
    Returns:
        Dict[str, Any]: Analysis results including scores and code smells
    """
    try:
        # Check if file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Read the file content
        with open(file_path, 'r', encoding='utf-8') as f:
            code_content = f.read()
        
        # Create analyzer and analyze
        analyzer = DataEngineeringCodeAnalyzer()
        result = analyzer.analyze_code(code_content, file_path)
        
        # Save results to files
        base_name = os.path.splitext(file_path)[0]
        
        # Save JSON results
        json_output = f"{base_name}_analysis.json"
        with open(json_output, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2)
        
        # Save readable report
        report = analyzer.generate_report(result)
        report_output = f"{base_name}_report.txt"
        with open(report_output, 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(f"✅ Analysis completed for: {file_path}")
        print(f"📊 Overall Quality Score: {result['overall_score']}/5.0")
        print(f"🐛 Total Issues Found: {result['total_smells']}")
        print(f"🚨 Critical Issues: {result['critical_smells']}")
        print(f"📄 JSON Report saved to: {json_output}")
        print(f"📋 Readable Report saved to: {report_output}")
        print("\n" + "="*60)
        print("SUMMARY REPORT:")
        print("="*60)
        print(report)
        
        return result
        
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        return None
    except UnicodeDecodeError:
        print(f"❌ Error: Cannot read file {file_path}. Please ensure it's a text file with UTF-8 encoding.")
        return None
    except Exception as e:
        print(f"❌ Unexpected error analyzing {file_path}: {e}")
        return None


def main():
    """Main function - accepts file path as command line argument or prompts user"""
    import sys
    
    print("🔍 Data Engineering PySpark Code Quality Analyzer")
    print("="*50)
    
    # Check if file path provided as command line argument
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        print(f"📁 Analyzing file: {file_path}")
        analyze_file_from_path(file_path)
    else:
        # Interactive mode - ask user for file path
        while True:
            print("\nOptions:")
            print("1. Analyze a specific file")
            print("2. Exit")
            
            choice = input("\nEnter your choice (1-2): ").strip()
            
            if choice == "1":
                file_path = input("📁 Enter the path to your PySpark file: ").strip()
                if file_path:
                    # Remove quotes if user added them
                    file_path = file_path.strip('"').strip("'")
                    analyze_file_from_path(file_path)
                else:
                    print("❌ Please provide a valid file path.")
            
            elif choice == "2":
                print("👋 Goodbye!")
                break
            
            else:
                print("❌ Invalid choice. Please enter 1 or 2.")


if __name__ == "__main__":
    import os
    main()
# PySpark Code Quality Analyzer

A comprehensive tool for analyzing PySpark code against Data Engineering best practices and coding guidelines. This analyzer provides quality scoring, code smell identification, and specific suggestions for improvement across modern cloud-based data platforms including Databricks, Microsoft Fabric, and other distributed computing environments.

## 🎯 Overview

This tool analyzes PySpark code against Data Engineering coding standards, providing automated quality scoring, code smell detection, and specific improvement recommendations. It's designed for data engineers, analytics engineers, and data platform teams working on distributed data processing pipelines with a focus on code quality, performance optimization, and maintainable data architectures.

## 🚀 Features

- **Quality Scoring**: 1-5 scale evaluation across 8 key categories
- **Code Smell Detection**: Identifies issues with criticality levels (LOW, MEDIUM, HIGH, CRITICAL)
- **Priority-Based Recommendations**: P1 (Fix Immediately) to P4 (Fix When Convenient)
- **Comprehensive Analysis**: Covers structure, performance, error handling, and more
- **Multiple Output Formats**: JSON for automation, human-readable reports
- **Command Line & Interactive Modes**: Flexible usage options
- **CI/CD Integration**: Ready for automated quality gates
- **Zero Dependencies**: Uses only Python standard library

## 📊 Analysis Categories

| Category | Weight | Focus Area |
|----------|--------|------------|
| **Structure & Organization** | 20% | Modularity, notebook structure, function design |
| **Error Handling** | 20% | Try-catch blocks, logging, retry logic |
| **Function Design** | 15% | Function-based approach, docstrings |
| **Performance** | 15% | Avoiding anti-patterns, caching, joins |
| **Delta Lake Usage** | 15% | Format usage, SCD implementation |
| **Logging** | 10% | Proper logging practices vs print statements |
| **Naming Conventions** | 3% | snake_case standards |
| **Documentation** | 2% | Comments and header documentation |

## 🛠️ Installation

### Prerequisites
- Python 3.7 or higher
- No external dependencies required (uses only standard library)

### Setup
```bash
# Clone the repository
git clone https://github.com/nikosreppaseb/code-analyzer.git
cd code-analyzer

# Make the script executable (optional)
chmod +x analyzer.py
```

## 📖 Usage

### Command Line Mode
```bash
# Analyze a specific file
python analyzer.py path/to/your/pyspark_file.py

# Example
python analyzer.py examples/sample_pipeline.py
```

### Interactive Mode
```bash
# Run without arguments for interactive mode
python analyzer.py
```

The interactive mode will prompt you to:
1. Choose to analyze a file
2. Enter the file path
3. View results and reports

### Python Script Integration
```python
from data_engineering_analyzer import analyze_file_from_path

# Analyze any file by providing its path
result = analyze_file_from_path("/path/to/your/pyspark_notebook.py")

if result:
    print(f"Quality Score: {result['overall_score']}/5.0")
    print(f"Issues Found: {result['total_smells']}")
    print(f"Critical Issues: {result['critical_smells']}")
```

## 📋 Output Files

For each analyzed file, the tool generates:

1. **JSON Report**: `{filename}_analysis.json`
   - Machine-readable format
   - Complete analysis data
   - Suitable for CI/CD integration

2. **Human-Readable Report**: `{filename}_report.txt`
   - Formatted text report
   - Organized by priority
   - Detailed suggestions

## 📈 Sample Output

```
✅ Analysis completed for: sample_pipeline.py
📊 Overall Quality Score: 3.2/5.0
🐛 Total Issues Found: 12
🚨 Critical Issues: 2
📄 JSON Report saved to: sample_pipeline_analysis.json
📋 Readable Report saved to: sample_pipeline_report.txt
```

## 🎯 Quality Grades

| Score Range | Grade | Description |
|-------------|-------|-------------|
| 4.5 - 5.0 | A+ | Excellent |
| 4.0 - 4.4 | A | Very Good |
| 3.5 - 3.9 | B+ | Good |
| 3.0 - 3.4 | B | Fair |
| 2.5 - 2.9 | C+ | Below Average |
| 2.0 - 2.4 | C | Poor |
| < 2.0 | D | Critical Issues |

## 🔍 Code Smell Examples

### Structure & Organization
- ❌ Missing `execute_main_pipeline()` function
- ❌ Large functions (>100 lines)
- ❌ Missing proper imports

### Performance Issues
- ❌ Using `.collect()` on large DataFrames
- ❌ Unnecessary `.count()` operations
- ❌ Missing `.unpersist()` after caching

### Error Handling
- ❌ I/O operations not wrapped in try-except
- ❌ Exceptions caught but not logged
- ❌ Missing retry logic

### Delta Lake
- ❌ Using Parquet instead of Delta format
- ❌ SCD implementation without proper merge logic

## 🔧 Integration with CI/CD

### GitHub Actions Example
```yaml
name: Code Quality Check
on: [push, pull_request]

jobs:
  code-quality:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v2
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.8'
    - name: Run PySpark Code Quality Analyzer
      run: |
        python analyzer.py src/main_pipeline.py
        # Fail if critical issues found
        python -c "
        import json
        with open('src/main_pipeline_analysis.json') as f:
            result = json.load(f)
        if result['critical_smells'] > 0:
            print(f'❌ Found {result[\"critical_smells\"]} critical issues!')
            exit(1)
        print('✅ No critical issues found')
        "
```

### Jenkins Pipeline
```groovy
pipeline {
    agent any
    stages {
        stage('Code Quality Analysis') {
            steps {
                script {
                    sh 'python analyzer.py src/pipeline.py'
                    
                    def analysis = readJSON file: 'src/pipeline_analysis.json'
                    
                    if (analysis.critical_smells > 0) {
                        error("Critical code quality issues found: ${analysis.critical_smells}")
                    }
                    
                    echo "Quality Score: ${analysis.overall_score}/5.0"
                }
            }
        }
    }
}
```

## 📝 Best Practices Enforced

### 1. Function-Based Design
```python
# ❌ Avoid inline transformations
df = df.withColumn("new_col", F.lit("value")).filter(F.col("status") == "active")

# ✅ Use functions
def apply_business_rules(df):
    """Apply standard business rules to the dataset."""
    return df.withColumn("new_col", F.lit("value")).filter(F.col("status") == "active")
```

### 2. Proper Error Handling
```python
# ❌ Risky operation without error handling
df = spark.read.parquet(input_path)

# ✅ Wrapped in try-except with logging
try:
    df = spark.read.parquet(input_path)
    logging.info(f"Successfully read data from {input_path}")
except Exception as e:
    logging.error(f"Failed to read data from {input_path}: {e}", exc_info=True)
    raise
```

### 3. Delta Lake Usage
```python
# ❌ Using Parquet
df.write.format("parquet").save(output_path)

# ✅ Using Delta
df.write.format("delta").save(output_path)
```

### 4. Performance Optimization
```python
# ❌ Collecting large datasets
all_data = df.collect()

# ✅ Limiting for debugging
sample_data = df.limit(100).toPandas()

# ❌ Missing unpersist
cached_df = df.cache()
# ... operations ...

# ✅ Proper cache management
cached_df = df.cache()
# ... operations ...
cached_df.unpersist()
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/improvement`)
3. Make your changes
4. Add tests if applicable
5. Commit your changes (`git commit -am 'Add new feature'`)
6. Push to the branch (`git push origin feature/improvement`)
7. Create a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🐛 Issues and Support

- **Bug Reports**: [GitHub Issues](https://github.com/nikosreppaseb/code-analyzer/issues)
- **Feature Requests**: [GitHub Issues](https://github.com/nikosreppaseb/code-analyzer/issues)
- **Documentation**: [Wiki](https://github.com/nikosreppaseb/code-analyzer/wiki)

## 📚 Additional Resources

- [Data Engineering Best Practices](docs/best-practices.md)
- [PySpark Performance Guidelines](docs/performance-guide.md)
- [Delta Lake Implementation Guide](docs/delta-lake-guide.md)

## 🏷️ Version History

- **v1.0.0** - Initial release with core analysis features
- **v1.1.0** - Added Delta Lake analysis
- **v1.2.0** - Performance optimization checks
- **v1.3.0** - Enhanced error handling analysis

## 📞 Contact

- **Author**: Nikos Reppas
- **GitHub**: [@nikosreppaseb](https://github.com/nikosreppaseb)

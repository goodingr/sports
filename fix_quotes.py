"""Fix escaped quotes in engine.py"""
with open('src/predict/engine.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace escaped quotes
content = content.replace('row[\\"', 'row["').replace('\\"]', '"]')

with open('src/predict/engine.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Fixed escaped quotes")

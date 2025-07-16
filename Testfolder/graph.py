import graphviz

# Define the graph
dot = graphviz.Digraph(format='png')
dot.attr(rankdir='TD', style='filled')

# Define nodes with labels and colors
nodes = {
    'A': ('Project Kick-off & API Setup', '#D0E0F0'),
    'B': ('API Credentials Obtained & Test Fetch Successful', '#FFFFCC'),
    'C': ('Full Data Acquisition: Pagination & Rate Limits Handled', '#D0F0D0'),
    'D': ('Store Raw Data (CSV/SQLite)', '#D0F0D0'),
    'E': ('Data Cleaning & Feature Engineering', '#D0F0D0'),
    'F': ('Prompt Engineering & Gen AI Integration (Sentiment, Topics)', '#F0D0F0'),
    'G': ('Store Enriched Data with Gen AI Insights', '#D0F0D0'),
    'H': ('Power BI Data Connection & Modeling', '#E0E0FF'),
    'I': ('Exploratory Data Analysis (EDA) & Dashboard Design', '#E0E0FF'),
    'J': ('Interactive Power BI Dashboard Development', '#E0E0FF'),
    'K': ('Dashboard Finalization & Testing', '#FFFFCC'),
    'L': ('Final Presentation & Project Retrospective', '#D0E0F0'),
}

# Add nodes
for node_id, (label, color) in nodes.items():
    dot.node(node_id, label=label, style='filled', fillcolor=color, color='#333', penwidth='2')

# Add edges
edges = [
    ('A', 'B'), ('B', 'C'), ('C', 'D'), ('D', 'E'), ('E', 'F'),
    ('F', 'G'), ('G', 'H'), ('H', 'I'), ('I', 'J'), ('J', 'K'), ('K', 'L')
]

for src, dst in edges:
    dot.edge(src, dst)

# Render diagram to file
dot.render('graph', format='png', cleanup=False)

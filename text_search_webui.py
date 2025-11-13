#!/usr/bin/env python3
"""
Database-based Text Search Tool for Epstein Documents

This tool allows you to search through the TEXT directory using the SQLite database
and view results with configurable snippet length around the matching text.
"""

import os
import re
import sqlite3
from flask import Flask, render_template, request, jsonify
import sys
from pathlib import Path

app = Flask(__name__)

# Database path
DB_PATH = "text_search.db"

def search_database(query, snippet_length=1000, search_type="content"):
    """
    Search for the query in the database.

    Args:
        query (str): The search query
        snippet_length (int): Number of characters before and after the match to include in snippet
        search_type (str): 'content', 'filename', or 'all'

    Returns:
        list: List of dictionaries with search results
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # Prepare FTS query based on search type
        # For phrase search (multi-word queries), wrap in quotes to match the exact phrase
        if ' ' in query:
            # Escape double quotes in the query and wrap in quotes for phrase matching
            escaped_query = query.replace('"', '""')
            quoted_query = f'"{escaped_query}"'
        else:
            # For single words, use the original query
            quoted_query = query

        if search_type == "content":
            fts_query = f"content:{quoted_query}"
            cursor.execute(
                '''
                SELECT
                    tf.filepath,
                    tf.filename,
                    tf.content,
                    text_files_fts.rank
                FROM text_files_fts
                JOIN text_files AS tf ON text_files_fts.rowid = tf.id
                WHERE text_files_fts MATCH ?
                ORDER BY text_files_fts.rank
                LIMIT 10000
                ''',
                (fts_query,)
            )
        elif search_type == "filename":
            fts_query = f"filename:{quoted_query}"
            cursor.execute(
                '''
                SELECT
                    tf.filepath,
                    tf.filename,
                    tf.content,
                    text_files_fts.rank
                FROM text_files_fts
                JOIN text_files AS tf ON text_files_fts.rowid = tf.id
                WHERE text_files_fts MATCH ?
                ORDER BY text_files_fts.rank
                LIMIT 10000
                ''',
                (fts_query,)
            )
        elif search_type == "all":
            fts_query = f"content:{quoted_query} OR filename:{quoted_query}"
            cursor.execute(
                '''
                SELECT
                    tf.filepath,
                    tf.filename,
                    tf.content,
                    text_files_fts.rank
                FROM text_files_fts
                JOIN text_files AS tf ON text_files_fts.rowid = tf.id
                WHERE text_files_fts MATCH ?
                ORDER BY text_files_fts.rank
                LIMIT 10000
                ''',
                (fts_query,)
            )
        else:
            # Default to content search
            fts_query = f"content:{quoted_query}"
            cursor.execute(
                '''
                SELECT
                    tf.filepath,
                    tf.filename,
                    tf.content,
                    text_files_fts.rank
                FROM text_files_fts
                JOIN text_files AS tf ON text_files_fts.rowid = tf.id
                WHERE text_files_fts MATCH ?
                ORDER BY text_files_fts.rank
                LIMIT 10000
                ''',
                (fts_query,)
            )

        rows = cursor.fetchall()
        results = []

        # Process each result to create snippets
        # The content in the database is a sample for FTS, but for display we should load the full content
        for filepath, filename, db_content, rank in rows:
            # For display, we'll use the full content from the file, but for search, the DB has already matched
            try:
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    full_content = f.read()
            except Exception as e:
                print(f"Error reading full content of {filepath}: {e}", file=sys.stderr)
                full_content = db_content  # Fallback to DB content if file can't be read

            # Find matches in full content and create snippets
            query_regex = re.escape(query)
            pattern = re.compile(query_regex, re.IGNORECASE)
            matches = list(pattern.finditer(full_content))

            if matches:
                # Use the first match to create a snippet
                match = matches[0]
                start = max(0, match.start() - snippet_length)
                end = min(len(full_content), match.end() + snippet_length)
                snippet = full_content[start:end]

                # Add ellipsis if we truncated
                if start > 0:
                    snippet = "..." + snippet
                if end < len(full_content):
                    snippet = snippet + "..."

                # Highlight the query in the snippet for display
                highlighted_snippet = re.sub(
                    query_regex, 
                    r'<span class="highlight">\g<0></span>', 
                    snippet, 
                    flags=re.IGNORECASE
                )

                results.append({
                    'file_path': filepath,
                    'file_name': filename,
                    'snippet': highlighted_snippet,
                    'rank': rank,
                    'content_preview': full_content[:1000] + ("..." if len(full_content) > 1000 else "")  # Preview for display
                })
            else:
                # If no match found in full content (e.g., match was in filename), create a simple snippet from full content
                snippet = full_content[:snippet_length*2]
                if len(full_content) > snippet_length*2:
                    snippet = snippet + "..."

                # Still apply highlighting in case the query appears elsewhere in the snippet
                highlighted_snippet = re.sub(
                    query_regex, 
                    r'<span class="highlight">\g<0></span>', 
                    snippet, 
                    flags=re.IGNORECASE
                )

                results.append({
                    'file_path': filepath,
                    'file_name': filename,
                    'snippet': highlighted_snippet,
                    'rank': rank,
                    'content_preview': full_content[:1000] + ("..." if len(full_content) > 1000 else "")  # Preview for display
                })

        return results

    except Exception as e:
        print(f"Error searching database: {e}", file=sys.stderr)
        return []
    finally:
        conn.close()


@app.route('/')
def index():
    """Main page with search form"""
    return render_template('index.html')


@app.route('/search', methods=['POST'])
def search():
    """Handle search requests"""
    data = request.json
    query = data.get('query', '')
    snippet_length = int(data.get('snippet_length', 1000))
    search_type = data.get('search_type', 'content')  # Default to content search

    if not query:
        return jsonify({'error': 'Query is required'}), 400

    print(f"Searching for: '{query}' in {search_type} with snippet length: {snippet_length}", file=sys.stderr)
    results = search_database(query, snippet_length, search_type)
    print(f"Found {len(results)} results", file=sys.stderr)

    return jsonify({
        'query': query,
        'results': results,
        'count': len(results),
        'search_type': search_type
    })


@app.route('/view_file/<path:file_path>')
def view_file(file_path):
    """View the full content of a file"""
    import os
    # Handle potential missing leading slash from Flask's path converter
    # The file path received might be missing the leading slash
    if not file_path.startswith('/'):
        full_path = '/' + file_path
    else:
        full_path = file_path
    
    # First try with full path (with leading slash)
    if os.path.exists(full_path) and full_path.endswith('.txt'):
        try:
            with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Look for corresponding image file and generate a URL to serve it
            image_path = find_corresponding_image(full_path)
            image_url = None
            if image_path:
                filename = os.path.basename(image_path)
                # Find which directory number the image is in
                dirs_with_numbers = [f"00{i}" for i in range(1, 13)]
                for dir_num in dirs_with_numbers:
                    if f"/{dir_num}/" in image_path:
                        image_url = f"/view_image/{dir_num}/{filename}"
                        break
                # If not found in expected dir structure, try to extract from path
                if not image_url:
                    import re
                    match = re.search(r'/(\d{3})/([^/]+)$', image_path)
                    if match:
                        dir_part = match.group(1)
                        file_part = match.group(2)
                        image_url = f"/view_image/{dir_part}/{file_part}"
            
            return render_template('view_file.html',
                                 file_path=full_path,
                                 content=content,
                                 image_url=image_url)
        except Exception as e:
            return f"Error reading file: {e}", 500
    else:
        # If the full path doesn't work, try with additional URL decoding
        import urllib.parse
        decoded_path = urllib.parse.unquote(full_path)
        
        if os.path.exists(decoded_path) and decoded_path.endswith('.txt'):
            try:
                with open(decoded_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                
                # Look for corresponding image file and generate a URL to serve it
                image_path = find_corresponding_image(decoded_path)
                image_url = None
                if image_path:
                    filename = os.path.basename(image_path)
                    # Find which directory number the image is in
                    dirs_with_numbers = [f"00{i}" for i in range(1, 13)]
                    for dir_num in dirs_with_numbers:
                        if f"/{dir_num}/" in image_path:
                            image_url = f"/view_image/{dir_num}/{filename}"
                            break
                    # If not found in expected dir structure, try to extract from path
                    if not image_url:
                        import re
                        match = re.search(r'/(\d{3})/([^/]+)$', image_path)
                        if match:
                            dir_part = match.group(1)
                            file_part = match.group(2)
                            image_url = f"/view_image/{dir_part}/{file_part}"
                
                return render_template('view_file.html',
                                     file_path=decoded_path,
                                     content=content,
                                     image_url=image_url)
            except Exception as e:
                return f"Error reading file: {e}", 500
        else:
            return "File not found", 404


def find_corresponding_image(txt_path):
    """Find the corresponding image file for a text file"""
    import os
    import re
    
    # Extract the filename without extension and directory
    base_name = os.path.splitext(os.path.basename(txt_path))[0]
    
    # Define possible image extensions
    image_extensions = ['.jpg', '.jpeg', '.JPG', '.JPEG', '.png', '.PNG', '.gif', '.GIF', '.tif', '.tiff', '.TIFF', '.TIF']
    
    # Get the directory structure - we need to find which subdirectory this text file is from
    # Since text files are in /TEXT/00X/, we'll look for images in the corresponding number directory first
    match = re.search(r'/TEXT/(\d{3})/', txt_path)
    if match:
        sub_dir = match.group(1)  # e.g., "001", "002", etc.
        image_dir = f"/home/jon/Documents/Epstein dump nov 12/{sub_dir}"
        
        for ext in image_extensions:
            image_path = os.path.join(image_dir, f"{base_name}{ext}")
            if os.path.exists(image_path):
                return image_path
    
    # If not found in the matching directory, search in all numbered directories
    base_dir = "/home/jon/Documents/Epstein dump nov 12"
    for i in range(1, 13):  # 001 to 012
        dir_num = f"{i:03d}"  # Format as 001, 002, ..., 012
        image_dir = os.path.join(base_dir, dir_num)
        if os.path.isdir(image_dir):
            for ext in image_extensions:
                image_path = os.path.join(image_dir, f"{base_name}{ext}")
                if os.path.exists(image_path):
                    return image_path
    
    return None


@app.route('/view_image/<dir_num>/<filename>')
def view_image(dir_num, filename):
    """Serve image files"""
    import os
    
    # Validate directory number to prevent directory traversal
    if not dir_num.isdigit() or int(dir_num) < 1 or int(dir_num) > 12:
        return "Invalid directory", 400
    
    # Validate filename to prevent directory traversal
    if '..' in filename or filename.startswith('/'):
        return "Invalid filename", 400
    
    # Construct the full path
    base_path = f"/home/jon/Documents/Epstein dump nov 12/{dir_num}"
    full_path = os.path.join(base_path, filename)
    
    # Check if the file exists and is in the expected location
    if os.path.exists(full_path) and os.path.isfile(full_path):
        from flask import send_file
        return send_file(full_path)
    else:
        return "File not found", 404

def create_templates():
    """Create template files if they don't exist"""
    templates_dir = os.path.join(os.path.dirname(__file__), 'templates')
    os.makedirs(templates_dir, exist_ok=True)

    # Index template with improved UI for showing no results
    index_template = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Epstein Document Search - Database</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }

        .container {
            background-color: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }

        h1 {
            color: #333;
            text-align: center;
            margin-bottom: 30px;
        }

        .search-form {
            margin-bottom: 30px;
        }

        .form-group {
            margin-bottom: 15px;
        }

        label {
            display: block;
            margin-bottom: 5px;
            font-weight: bold;
        }

        input[type="text"], input[type="number"], select {
            width: 100%;
            padding: 10px;
            border: 1px solid #ddd;
            border-radius: 4px;
            box-sizing: border-box;
        }

        button {
            background-color: #007bff;
            color: white;
            padding: 10px 20px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 16px;
        }

        button:hover {
            background-color: #0056b3;
        }

        .results {
            margin-top: 30px;
        }

        .result {
            border: 1px solid #ddd;
            padding: 15px;
            margin-bottom: 15px;
            border-radius: 4px;
            background-color: #fafafa;
        }

        .file-link {
            font-weight: bold;
            color: #007bff;
            text-decoration: none;
            margin-bottom: 10px;
            display: inline-block;
        }

        .file-link:hover {
            text-decoration: underline;
        }

        .snippet {
            margin: 10px 0;
            line-height: 1.5;
            font-family: monospace;
            white-space: pre-wrap;
        }

        .highlight {
            background-color: yellow;
            font-weight: bold;
        }

        .loading {
            text-align: center;
            padding: 20px;
            display: none;
        }

        .result-count {
            margin-top: 15px;
            font-size: 14px;
            color: #666;
            font-weight: bold;
        }

        .error-message {
            color: #dc3545;
            font-weight: bold;
            margin: 15px 0;
        }
        
        .search-type-info {
            font-size: 12px;
            color: #666;
            margin-top: 5px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Epstein Document Search - Database</h1>

        <form class="search-form" id="searchForm">
            <div class="form-group">
                <label for="query">Search Query:</label>
                <input type="text" id="query" name="query" required placeholder="Enter search term (e.g., 'Epstein', 'Jeffrey', 'sex', etc.)">
            </div>

            <div class="form-group">
                <label for="search_type">Search Type:</label>
                <select id="search_type" name="search_type">
                    <option value="content" selected>Content Only</option>
                    <option value="filename">Filename Only</option>
                    <option value="all">Content and Filename</option>
                </select>
                <div class="search-type-info">Content Only is recommended for best performance</div>
            </div>

            <div class="form-group">
                <label for="snippet_length">Snippet Length (characters before/after match):</label>
                <input type="number" id="snippet_length" name="snippet_length" value="1000" min="10" max="2000">
            </div>

            <button type="submit">Search</button>
        </form>

        <div class="loading" id="loading">Searching...</div>

        <div class="results" id="results"></div>
    </div>

    <script>
        document.getElementById('searchForm').addEventListener('submit', async function(e) {
            e.preventDefault();

            const query = document.getElementById('query').value;
            const snippetLength = document.getElementById('snippet_length').value;
            const searchType = document.getElementById('search_type').value;

            if (!query.trim()) {
                alert('Please enter a search query');
                return;
            }

            // Show loading indicator
            document.getElementById('loading').style.display = 'block';
            document.getElementById('results').innerHTML = '';

            try {
                const response = await fetch('/search', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        query: query,
                        snippet_length: parseInt(snippetLength),
                        search_type: searchType
                    })
                });

                const data = await response.json();

                if (data.error) {
                    document.getElementById('results').innerHTML = `<div class="error-message">Error: ${data.error}</div>`;
                } else {
                    displayResults(data);
                }
            } catch (error) {
                document.getElementById('results').innerHTML = `<div class="error-message">Error: ${error.message}</div>`;
            } finally {
                document.getElementById('loading').style.display = 'none';
            }
        });

        function displayResults(data) {
            const resultsContainer = document.getElementById('results');

            if (data.count === 0) {
                resultsContainer.innerHTML = '<p>No results found. Try another search term.</p>';
                return;
            }

            let html = `<div class="result-count">${data.count} result${data.count !== 1 ? 's' : ''} found for "${data.query}" (searched in ${data.search_type})</div>`;

            data.results.forEach(result => {
                html += `
                <div class="result">
                    <a href="/view_file/${encodeURIComponent(result.file_path)}" class="file-link" target="_blank">
                        ${result.file_name}
                    </a>
                    <div class="snippet">${result.snippet}</div>
                </div>
                `;
            });

            resultsContainer.innerHTML = html;
        }
    </script>
</body>
</html>'''

    # View file template
    view_file_template = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ file_path }} - Epstein Document Viewer</title>
    <style>
        body {
            font-family: monospace;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }

        .container {
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }

        .file-header {
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 1px solid #eee;
        }

        .back-link {
            display: inline-block;
            margin-bottom: 10px;
            color: #007bff;
            text-decoration: none;
        }

        .back-link:hover {
            text-decoration: underline;
        }
        
        .image-link {
            margin-top: 10px;
        }
        
        .image-link-button {
            display: inline-block;
            background-color: #28a745;
            color: white;
            padding: 8px 16px;
            border-radius: 4px;
            text-decoration: none;
            font-weight: bold;
        }
        
        .image-link-button:hover {
            background-color: #218838;
            text-decoration: none;
        }

        .file-content {
            white-space: pre-wrap;
            line-height: 1.4;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="file-header">
            <a href="/" class="back-link">← Back to Search</a>
            <h1>{{ file_path | e }}</h1>
            {% if image_url %}
            <div class="image-link">
                <a href="{{ image_url }}" target="_blank" class="image-link-button">View Corresponding Image</a>
            </div>
            {% endif %}
        </div>

        <div class="file-content">{{ content | e }}</div>
    </div>
</body>
</html>'''

    # Write the templates to files
    with open(os.path.join(templates_dir, 'index.html'), 'w', encoding='utf-8') as f:
        f.write(index_template)

    with open(os.path.join(templates_dir, 'view_file.html'), 'w', encoding='utf-8') as f:
        f.write(view_file_template)

    print(f"Templates created in {templates_dir}")


if __name__ == '__main__':
    # Check if database exists
    if not os.path.exists(DB_PATH):
        print(f"Error: Database file '{DB_PATH}' not found.")
        print("Please make sure the database has been created with the indexing script.")
        sys.exit(1)

    # Create templates before starting the app
    create_templates()

    print("Starting Epstein Document Search Tool...")
    print("Visit http://localhost:5000 to use the search interface")

    # Start the Flask app
    app.run(host='0.0.0.0', port=5000, debug=False)
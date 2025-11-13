#!/usr/bin/env python3
"""
Highly optimized searchable database for text files in the TEXT directory.
This script creates an SQLite database with full-text search capability
for all text files in the TEXT directory and subdirectories.
Uses on-demand content loading to minimize memory usage.
"""

import os
import sqlite3
import glob
from pathlib import Path
import sys
from typing import List, Tuple


class TextSearchDatabase:
    def __init__(self, db_path: str = "text_search.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.create_tables()

    def create_tables(self):
        """Create the database table with full-text search support."""
        # Enable FTS5 (full-text search) in SQLite
        self.conn.execute('PRAGMA foreign_keys = ON')

        # Create regular table for metadata
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS text_files (
                id INTEGER PRIMARY KEY,
                filename TEXT NOT NULL,
                filepath TEXT NOT NULL,
                content TEXT  -- This will be NULL during indexing to save memory
            )
        ''')

        # Create FTS5 table for full-text search
        # We'll store a portion of content for searching during indexing
        self.conn.execute('''
            CREATE VIRTUAL TABLE IF NOT EXISTS text_files_fts
            USING fts5(id, content, filename, filepath, content='text_files', content_rowid='id')
        ''')

        # Create triggers to keep FTS table in sync with main table
        self.conn.executescript('''
            CREATE TRIGGER IF NOT EXISTS text_files_ai AFTER INSERT ON text_files
            BEGIN
                INSERT INTO text_files_fts(rowid, id, content, filename, filepath)
                VALUES (new.id, new.id, new.content, new.filename, new.filepath);
            END;

            CREATE TRIGGER IF NOT EXISTS text_files_ad AFTER DELETE ON text_files
            BEGIN
                INSERT INTO text_files_fts(text_files_fts, rowid, id, content, filename, filepath)
                VALUES('delete', old.id, old.id, old.content, old.filename, old.filepath);
            END;

            CREATE TRIGGER IF NOT EXISTS text_files_au AFTER UPDATE ON text_files
            BEGIN
                INSERT INTO text_files_fts(text_files_fts, rowid, id, content, filename, filepath)
                VALUES('delete', old.id, old.id, old.content, old.filename, old.filepath);
                INSERT INTO text_files_fts(rowid, id, content, filename, filepath)
                VALUES (new.id, new.id, new.content, new.filename, new.filepath);
            END;
        ''')

        self.conn.commit()

    def index_text_files(self, text_directory: str, batch_size: int = 100, content_sample_size: int = 10240):
        """
        Index all text files in the given directory and subdirectories.
        Only store a small sample of content for search indexing to save memory.
        """
        print(f"Indexing text files from {text_directory}...")

        # Find all .txt files in the directory and subdirectories
        txt_files = glob.glob(os.path.join(text_directory, "**", "*.txt"), recursive=True)
        total_files = len(txt_files)

        print(f"Found {total_files} text files to index...")

        # Process files in batches to reduce memory usage
        for i in range(0, total_files, batch_size):
            batch = txt_files[i:i + batch_size]
            
            # Process each file in the batch
            for j, file_path in enumerate(batch, i + 1):
                try:
                    # Read only a small sample of the file content for indexing
                    content_sample = self._read_file_content_sample(file_path, content_sample_size)
                    filename = os.path.basename(file_path)

                    # Insert into database with content sample for search, filepath for loading full content later
                    self.conn.execute(
                        "INSERT INTO text_files (filename, filepath, content) VALUES (?, ?, ?)",
                        (filename, file_path, content_sample)  # Store only the sample for search indexing
                    )

                    if j % 100 == 0 or j == total_files:
                        print(f"Indexed {j}/{total_files} files ({j/total_files*100:.1f}%)")

                except Exception as e:
                    print(f"Error processing {file_path}: {str(e)}")
                    continue

            # Commit every batch to free up memory
            self.conn.commit()
            print(f"Committed batch {i//batch_size + 1}/{(total_files + batch_size - 1)//batch_size}")

        print(f"Indexing complete! Indexed {total_files} files.")

    def _read_file_content_sample(self, file_path: str, sample_size: int = 10240) -> str:
        """Read a sample of the file content for indexing purposes."""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                # Read only the first part of the file for indexing
                sample = f.read(sample_size)
                return sample
        except Exception as e:
            print(f"Error reading file {file_path}: {str(e)}")
            return ""

    def load_full_content(self, filepath: str) -> str:
        """Load the full content of a file on demand."""
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                return f.read()
        except Exception as e:
            print(f"Error loading file {filepath}: {str(e)}")
            return ""

    def search(self, query: str, limit: int = 10000) -> List[Tuple[str, str, str, float]]:
        """Search the database for the given query."""
        cursor = self.conn.cursor()

        # Use FTS to search for the query in the sample content
        # For phrase search (multi-word queries), wrap in quotes
        if ' ' in query:
            escaped_query = query.replace('"', '""')  # Escape double quotes
            quoted_query = f'"{escaped_query}"'
            fts_query = f"content:{quoted_query} OR filename:{quoted_query}"
        else:
            fts_query = f"content:{query} OR filename:{query}"
            
        cursor.execute(
            '''
            SELECT
                tf.filename,
                tf.filepath,
                tf.content,  -- This is the sample content
                text_files_fts.rank
            FROM text_files_fts
            JOIN text_files AS tf ON text_files_fts.rowid = tf.id
            WHERE text_files_fts MATCH ?
            ORDER BY text_files_fts.rank
            LIMIT ?
            ''',
            (fts_query, limit)
        )

        results = cursor.fetchall()
        
        # Replace the sample content with full content for display
        full_results = []
        for filename, filepath, sample_content, rank in results:
            full_content = self.load_full_content(filepath)
            full_results.append((filename, filepath, full_content, rank))
        
        return full_results

    def search_content_only(self, query: str, limit: int = 10000) -> List[Tuple[str, str, str, float]]:
        """Search only in the content of documents."""
        cursor = self.conn.cursor()

        # Use FTS to search in content only
        # For phrase search (multi-word queries), wrap in quotes
        if ' ' in query:
            escaped_query = query.replace('"', '""')  # Escape double quotes
            quoted_query = f'"{escaped_query}"'
        else:
            quoted_query = query
            
        cursor.execute(
            '''
            SELECT
                tf.filename,
                tf.filepath,
                tf.content,
                text_files_fts.rank
            FROM text_files_fts
            JOIN text_files AS tf ON text_files_fts.rowid = tf.id
            WHERE text_files_fts MATCH ?
            ORDER BY text_files_fts.rank
            LIMIT ?
            ''',
            (f"content:{quoted_query}", limit)
        )

        results = cursor.fetchall()
        
        # Replace the sample content with full content for display
        full_results = []
        for filename, filepath, sample_content, rank in results:
            full_content = self.load_full_content(filepath)
            full_results.append((filename, filepath, full_content, rank))
        
        return full_results

    def search_filename_only(self, query: str, limit: int = 10000) -> List[Tuple[str, str, str, float]]:
        """Search only in the filenames."""
        cursor = self.conn.cursor()

        # Use FTS to search in filename only
        # For phrase search (multi-word queries), wrap in quotes
        if ' ' in query:
            escaped_query = query.replace('"', '""')  # Escape double quotes
            quoted_query = f'"{escaped_query}"'
        else:
            quoted_query = query
            
        cursor.execute(
            '''
            SELECT
                tf.filename,
                tf.filepath,
                tf.content,
                text_files_fts.rank
            FROM text_files_fts
            JOIN text_files AS tf ON text_files_fts.rowid = tf.id
            WHERE text_files_fts MATCH ?
            ORDER BY text_files_fts.rank
            LIMIT ?
            ''',
            (f"filename:{quoted_query}", limit)
        )

        results = cursor.fetchall()
        
        # Replace the sample content with full content for display
        full_results = []
        for filename, filepath, sample_content, rank in results:
            full_content = self.load_full_content(filepath)
            full_results.append((filename, filepath, full_content, rank))
        
        return full_results

    def count_files(self) -> int:
        """Get the total number of indexed files."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM text_files")
        return cursor.fetchone()[0]

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()


def main():
    db = TextSearchDatabase()

    if len(sys.argv) > 1 and sys.argv[1] == "index":
        # Index all text files
        text_dir = "/home/jon/Documents/Epstein dump nov 12/TEXT"
        if len(sys.argv) > 2:
            text_dir = sys.argv[2]

        if not os.path.exists(text_dir):
            print(f"Error: Directory {text_dir} does not exist!")
            sys.exit(1)

        db.index_text_files(text_dir)
        print(f"Database created with {db.count_files()} files indexed.")
    else:
        # Interactive search mode
        print("Text Search Database")
        print("====================")
        print(f"Database contains {db.count_files()} indexed files.")
        print("\nCommands:")
        print("  'search <query>' - Search in content only (default)")
        print("  'all <query>' - Search in content and filename")
        print("  'content <query>' - Search in content only")
        print("  'filename <query>' - Search in filename only")
        print("  'quit' or 'exit' - Exit the program")
        print("\nExample: search Epstein")
        print("Example: all Clinton")
        print("Example: filename 010477")

        while True:
            try:
                user_input = input("\nEnter search command: ").strip()

                if user_input.lower() in ['quit', 'exit', 'q']:
                    break

                if not user_input:
                    continue

                parts = user_input.split(' ', 1)
                if len(parts) < 2:
                    print("Please provide a search query. Example: search Epstein")
                    continue

                command = parts[0].lower()
                query = parts[1]

                if command == 'search':
                    results = db.search_content_only(query)  # Default to content search
                elif command == 'content':
                    results = db.search_content_only(query)
                elif command == 'filename':
                    results = db.search_filename_only(query)
                elif command == 'all':
                    results = db.search(query)  # Search both content and filename
                else:
                    print(f"Unknown command: {command}. Use 'search' (content only, default), 'all' (content and filename), 'content', or 'filename'.")
                    continue

                if not results:
                    print("No results found.")
                    continue

                print(f"\nFound {len(results)} results for '{query}':")
                print("-" * 80)

                for i, (filename, filepath, content, rank) in enumerate(results, 1):
                    # Show a preview of the content around the search term
                    content_preview = content[:500]  # First 500 chars
                    if len(content) > 500:
                        content_preview += "..."

                    print(f"\n{i}. File: {filename}")
                    print(f"   Path: {filepath}")
                    print(f"   Preview: {content_preview}")
                    print(f"   Rank: {rank}")
                    print("-" * 80)

            except KeyboardInterrupt:
                print("\nExiting...")
                break
            except Exception as e:
                print(f"Error: {str(e)}")

    db.close()


if __name__ == "__main__":
    main()
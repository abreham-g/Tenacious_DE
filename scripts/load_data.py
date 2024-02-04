import os
import sqlite3
import pandas as pd
import logging
import PyPDF2
from docx import Document

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LoadData:
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            # Connect to SQLite database or create if it doesn't exist
            self.conn = sqlite3.connect(self.db_name)
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to database: {self.db_name}")
        except sqlite3.Error as e:
            logger.error(f"Error connecting to database: {e}")

    def sanitize_name(self, name):
        # Replace characters that are not suitable for SQL table/column names
        sanitized_name = name.replace(" ", "_").replace(":", "_").replace("-", "_")
        
        # Remove leading digits from the column name
        sanitized_name = ''.join([c for c in sanitized_name if not c.isdigit() or c == '_'])

        return sanitized_name

    def create_table(self, table_name, columns):
        try:
            # Sanitize table name
            table_name = self.sanitize_name(table_name)
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
            self.cursor.execute(create_table_query)
            logger.info(f"Table created: {table_name}")
        except sqlite3.Error as e:
            logger.error(f"Error creating table: {e}")

    def insert_data_from_excel(self, table_name, excel_file_path):
        try:
            # Sanitize table name
            table_name = self.sanitize_name(table_name)
            df = pd.read_excel(excel_file_path)

            # Generate columns dynamically based on the DataFrame
            # Include a suffix to handle duplicate column names
            columns = ", ".join(f"{self.sanitize_name(col)}_{i} TEXT" for i, col in enumerate(df.columns) if col)

            self.create_table(table_name, columns)
        
            df.to_sql(table_name, self.conn, if_exists='replace', index=False)
            logger.info(f"Data from Excel inserted into table: {table_name}")
        except Exception as e:
            logger.error(f"Error inserting data from Excel: {e}")

    def extract_text_from_pdf(self, pdf_file_path):
        try:
            with open(pdf_file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfFileReader(file)
                text = ""
                for page_num in range(pdf_reader.numPages):
                    text += pdf_reader.getPage(page_num).extractText()
            return text
        except Exception as e:
            logger.error(f"Error extracting text from PDF: {e}")
            return ""

    def extract_text_from_docx(self, docx_file_path):
        try:
            doc = Document(docx_file_path)
            text = ""
            for paragraph in doc.paragraphs:
                text += paragraph.text + "\n"
            return text
        except Exception as e:
            logger.error(f"Error extracting text from DOCX: {e}")
            return ""

    def insert_data_from_pdf_or_docx(self, table_name, file_path):
        try:
            # Sanitize table name
            table_name = self.sanitize_name(table_name)
            
            # Extract text from PDF or DOCX file
            if file_path.lower().endswith('.pdf'):
                text = self.extract_text_from_pdf(file_path)
            elif file_path.lower().endswith('.docx'):
                text = self.extract_text_from_docx(file_path)
            else:
                logger.warning(f"Unsupported file format: {file_path}")
                return

            # Create a single column for text
            columns = "text TEXT"
            self.create_table(table_name, columns)

            # Insert text into the table
            self.cursor.execute(f"INSERT INTO {table_name} (text) VALUES (?)", (text,))
            logger.info(f"Text data inserted into table: {table_name}")
        except Exception as e:
            logger.error(f"Error inserting text data: {e}")

    def commit_and_close(self):
        try:
            # Commit changes and close the connection
            self.conn.commit()
            self.conn.close()
            logger.info("Connection closed.")
        except sqlite3.Error as e:
            logger.error(f"Error committing changes and closing connection: {e}")

def main():
    try:
        # Database name
        db_name = "Tenacious.db"
        data_loader = LoadData(db_name)
        data_loader.connect()
        data_directory = "../Data/"

        # Iterate through files in the directory a folder called (Data)
        for file_name in os.listdir(data_directory):
            file_path = os.path.join(data_directory, file_name)

            # Extract table name from file name (without extension)
            table_name = os.path.splitext(file_name)[0]

            if file_name.endswith(".xlsx"):
                # Insert data from Excel into the table
                data_loader.insert_data_from_excel(table_name, file_path)
            elif file_name.lower().endswith(('.pdf', '.docx')):
                
                data_loader.insert_data_from_pdf_or_docx(table_name, file_path)

        data_loader.commit_and_close()
    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

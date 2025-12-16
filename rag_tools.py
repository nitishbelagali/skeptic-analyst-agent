from langchain_community.document_loaders import PyPDFLoader
from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
from langchain.text_splitter import CharacterTextSplitter
from langchain_core.tools import tool

class RagSession:
    def __init__(self):
        self.vector_store = None
    
    def ingest_document(self, file_path):
        """Reads a PDF and builds the vector index."""
        try:
            loader = PyPDFLoader(file_path)
            documents = loader.load()
            if not documents:
                return "❌ Error: PDF appears empty."
                
            text_splitter = CharacterTextSplitter(chunk_size=1000, chunk_overlap=0)
            docs = text_splitter.split_documents(documents)
            
            embeddings = OpenAIEmbeddings()
            self.vector_store = FAISS.from_documents(docs, embeddings)
            return "✅ Data Dictionary ingested successfully."
        except Exception as e:
            return f"❌ RAG Error: {str(e)}"

    def search(self, query):
        """Searches the document for relevant context."""
        if not self.vector_store:
            return "❌ No dictionary loaded."
        try:
            docs = self.vector_store.similarity_search(query)
            if not docs:
                return "No relevant info found."
            return docs[0].page_content
        except Exception as e:
            return f"❌ Search Error: {str(e)}"

session = RagSession()

@tool
def consult_data_dictionary(query: str):
    """Searches the uploaded PDF guide for definitions of business terms."""
    return session.search(query)

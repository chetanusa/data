import snowflake.connector
import os
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_core.documents import Document
from dotenv import load_dotenv

load_dotenv()

def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
    )

def load_movies_from_snowflake():
    """Load all movies from Snowflake"""
    print("Loading movies from Snowflake...")
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    
    query = """
    SELECT 
        MOVIE_ID, TITLE, OVERVIEW, RELEASE_DATE,
        POPULARITY, RATING, RATING_COUNT,
        POPULARITY_TIER, RATING_LABEL
    FROM MOVIES
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    print(f"Loaded {len(results)} movies from Snowflake")
    return results

def create_movie_documents(movies):
    """Convert movie data to LangChain documents"""
    documents = []
    
    for movie in movies:
        movie_id, title, overview, release_date, popularity, rating, rating_count, pop_tier, rating_label = movie
        
        # Create text content for embedding
        content = f"""
        Title: {title}
        Overview: {overview if overview else 'No overview available'}
        Release Date: {release_date}
        Rating: {rating}/10 ({rating_label})
        Popularity: {popularity} ({pop_tier})
        """
        
        # Create metadata for filtering
        metadata = {
            "movie_id": movie_id,
            "title": title,
            "release_date": str(release_date),
            "rating": float(rating),
            "popularity": float(popularity),
            "rating_label": rating_label,
            "popularity_tier": pop_tier
        }
        
        doc = Document(page_content=content, metadata=metadata)
        documents.append(doc)
    
    print(f"Created {len(documents)} documents")
    return documents

def create_vector_store():
    """Create FAISS vector store from movie data"""
    print("Creating vector store...")
    
    # Load movies
    print("Step 1: Loading movies from Snowflake...")
    movies = load_movies_from_snowflake()
    print(f"✓ Loaded {len(movies)} movies")
    
    # Create documents
    print("Step 2: Creating documents...")
    documents = create_movie_documents(movies)
    print(f"✓ Created {len(documents)} documents")
    
    # Create embeddings
    print("Step 3: Creating embeddings (this may take 1-2 minutes)...")
    print("⏳ Making API calls to OpenAI...")
    
    embeddings = OpenAIEmbeddings(
        model="text-embedding-ada-002",
        openai_api_key=os.getenv('OPENAI_API_KEY')
    )
    
    # Create FAISS vector store
    print("Step 4: Building FAISS index...")
    vectorstore = FAISS.from_documents(documents, embeddings)
    
    # Save to disk
    print("Step 5: Saving to disk...")
    vectorstore.save_local("data/faiss_index")
    
    print("✅ Vector store created and saved to data/faiss_index")
    return vectorstore

def load_vector_store():
    """Load existing vector store from disk"""
    print("Loading vector store from disk...")
    embeddings = OpenAIEmbeddings(
        model="text-embedding-ada-002",
        openai_api_key=os.getenv('OPENAI_API_KEY')
    )
    
    vectorstore = FAISS.load_local(
        "data/faiss_index", 
        embeddings,
        allow_dangerous_deserialization=True
    )
    
    print("Vector store loaded successfully")
    return vectorstore

if __name__ == "__main__":
    # Run this once to create the vector store
    create_vector_store()
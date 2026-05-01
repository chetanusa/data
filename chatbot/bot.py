from embedings import load_vector_store, create_vector_store
from retriever import MovieRetriever
import os

class MovieChatbot:
    def __init__(self):
        # Load or create vector store
        if os.path.exists("data/faiss_index"):
            print("Loading existing vector store...")
            self.vectorstore = load_vector_store()
        else:
            print("Vector store not found. Creating new one...")
            self.vectorstore = create_vector_store()
        
        # Initialize retriever
        self.retriever = MovieRetriever(self.vectorstore)
    
    def ask(self, question):
        """Ask a question to the chatbot"""
        response = self.retriever.query(question)
        return response
    
    def reset(self):
        """Reset conversation history"""
        self.retriever.reset_memory()
        print("Conversation history cleared")

if __name__ == "__main__":
    # Test the chatbot
    bot = MovieChatbot()
    
    print("\n🎬 Movie Chatbot Ready!")
    print("Ask me anything about movies in the database.\n")
    
    while True:
        user_input = input("You: ")
        
        if user_input.lower() in ['quit', 'exit', 'bye']:
            print("Goodbye! 🎬")
            break
        
        if user_input.lower() == 'reset':
            bot.reset()
            continue
        
        response = bot.ask(user_input)
        print(f"\nBot: {response['answer']}\n")
        
        # Show source movies
        if response['source_documents']:
            print("📚 Sources:")
            for i, doc in enumerate(response['source_documents'][:3], 1):
                print(f"{i}. {doc.metadata['title']} (Rating: {doc.metadata['rating']})")
            print()
from langchain.chains import ConversationalRetrievalChain
from langchain_openai import ChatOpenAI
from langchain.memory import ConversationBufferMemory
from prompts import SYSTEM_PROMPT
import os
from dotenv import load_dotenv

load_dotenv()

class MovieRetriever:
    def __init__(self, vectorstore):
        self.vectorstore = vectorstore
        self.llm = ChatOpenAI(
            model="gpt-4",
            temperature=0.7,
            openai_api_key=os.getenv('OPENAI_API_KEY')
        )
        
        # Conversation memory
        self.memory = ConversationBufferMemory(
            memory_key="chat_history",
            return_messages=True,
            output_key="answer"
        )
        
        # Create retrieval chain
        self.chain = ConversationalRetrievalChain.from_llm(
            llm=self.llm,
            retriever=self.vectorstore.as_retriever(
                search_kwargs={"k": 5}  # Retrieve top 5 most relevant movies
            ),
            memory=self.memory,
            return_source_documents=True,
            verbose=True
        )
    
    def query(self, question):
        """Query the chatbot"""
        result = self.chain({"question": question})
        return {
            "answer": result["answer"],
            "source_documents": result["source_documents"]
        }
    
    def reset_memory(self):
        """Clear conversation history"""
        self.memory.clear()
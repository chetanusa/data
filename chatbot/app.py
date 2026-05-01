import streamlit as st
import sys
import os
sys.path.append(os.path.dirname(__file__))
from bot import MovieChatbot

st.set_page_config(
    page_title="🎬 TMDB Movie Chatbot",
    page_icon="🎬",
    layout="wide"
)

st.title("🎬 TMDB Movie Assistant")
st.markdown("Ask me anything about movies in the database!")

# Initialize chatbot
@st.cache_resource
def load_chatbot():
    return MovieChatbot()

chatbot = load_chatbot()

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Chat input
if prompt := st.chat_input("Ask about movies..."):
    # Add user message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Get bot response
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            response = chatbot.ask(prompt)
            st.markdown(response["answer"])
            
            # Show source movies
            if response["source_documents"]:
                with st.expander("📚 Source Movies"):
                    for i, doc in enumerate(response["source_documents"][:3], 1):
                        st.write(f"**{i}. {doc.metadata['title']}**")
                        st.write(f"Rating: {doc.metadata['rating']}/10 | Popularity: {doc.metadata['popularity_tier']}")
                        st.write("---")
    
    # Add assistant response
    st.session_state.messages.append({"role": "assistant", "content": response["answer"]})

# Sidebar
with st.sidebar:
    st.header("Options")
    if st.button("Clear Chat History"):
        st.session_state.messages = []
        chatbot.reset()
        st.rerun()
    
    st.markdown("---")
    st.markdown("### Example Questions:")
    st.markdown("- What are the top-rated movies?")
    st.markdown("- Tell me about Star Wars")
    st.markdown("- Recommend a sci-fi movie")
    st.markdown("- Compare Inception and Interstellar")
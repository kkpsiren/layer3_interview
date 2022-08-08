import streamlit as st
from landing import landing_page

from beautify import flipside_logo, discord_logo, set_bg_hack_url

import os


st.set_page_config(page_title="Layer3 x Across", layout="wide")
set_bg_hack_url()
landing_page()
# 
st.sidebar.markdown("#### About")
discord_logo(os.getenv('DISCORD_USERNAME'))
flipside_logo(url="https://flipsidecrypto.xyz/kipto-1822")
st.sidebar.markdown(f""" 
### 💻 Github
[kkpsiren/layer3_x_across](https://github.com/kkpsiren/layer3_x_across)  
    """)
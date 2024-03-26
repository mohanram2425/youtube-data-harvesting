# youtube-data-harvesting and warehousing
YouTube Data Harvesting and Warehousing

YouTube Data Harvesting and Warehousing is a project that enables users to access and analyze data from various YouTube channels. The project utilizes SQL and Streamlit to develop a user-friendly application for retrieving, storing, and querying YouTube channel and video data.
Tools and Libraries Used
Streamlit

Streamlit is used to create a user-friendly UI that allows users to interact with the application and perform data retrieval and analysis operations.
Python

Python is the primary programming language used for developing the entire application, including data retrieval, processing, analysis, and visualization.
Google API Client

The googleapiclient library in Python facilitates communication with different Google APIs. In this project, it is used to interact with YouTube's Data API v3 for retrieving essential information such as channel details, video specifics, and comments.
MySQL

MySQL is utilized as a relational database management system for storing structured data efficiently. It provides robust SQL capabilities for querying and managing data.
YouTube Data Scraping and Ethical Perspective

When scraping YouTube content, ethical considerations are crucial. It's essential to respect YouTube's terms and conditions, obtain appropriate authorization, and adhere to data protection regulations. Handling collected data responsibly ensures privacy, confidentiality, and prevents misuse or misrepresentation. Additionally, considering the impact on the platform and its community promotes fair and sustainable scraping practices.
Required Libraries

    googleapiclient.discovery
    streamlit
    mysql-connector-python
    pandas

Features

The YouTube Data Harvesting and Warehousing application offers the following features:

    Retrieval of channel and video data from YouTube using the YouTube API.
    Storage of data in a MySQL database for efficient querying and analysis.
    Search and retrieval of data from the MySQL database using different search options.

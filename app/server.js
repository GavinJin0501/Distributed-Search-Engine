// To start the server, run the following command in the terminal: node server.js
const express = require('express');
const cors = require('cors');
const app = express();
const PORT = 3000;

// Middleware to parse JSON bodies
app.use(express.json());
app.use(cors());
// Route to handle search
app.post('/search', (req, res) => {
    const searchTerm = req.body.searchTerm;
    console.log('Search Term Received:', searchTerm);
    // Here you could add logic to process the search term, e.g., querying a database
    // For simplicity, we just return the search term with a message
    res.json({ message: `Search complete for term: ${searchTerm}` });
});

// Start the server
app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});
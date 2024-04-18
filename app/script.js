document.getElementById('searchForm').addEventListener('submit', function(event) {
    event.preventDefault();
    var searchTerm = document.getElementById('searchInput').value;
    // trim the search term 
    searchTerm = searchTerm.trim();
    document.getElementById('resultList').textContent = 'Search for: ' + searchTerm;
    fetch('http://localhost:3000/search', {
        method:"POST",
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ searchTerm: searchTerm }),
    }).then(response => response.json())
    .then(data => {
        console.log(data);
        document.getElementById('resultList').textContent = data.message;
    });
});

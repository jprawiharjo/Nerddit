// custom javascript


$(function() {
  
  createGraph2();
});

function createGraph() {
 console.log('jquery is working!')
sigma.parsers.gexf('data/subreddits.gexf', {
  container: 'graph-container'
});
};

const initialState = {
  type: null,         // {'source', 'target', null}
  minSimilarity: 0,   // {0:100}
  maxSimilarity: 100, // {0:100}
  results: [],        // {arr}
  totalResults: 0,    // {int}
  err: null           // {str}
}

const searchReducer = (state = initialState, action) => {
  switch (action.type) {
    case 'RECEIVE_SEARCH_RESULTS':
      return Object.assign({}, state, {
        results: action.results.docs,
        totalResults: action.results.total,
        err: null
      })

    case 'SEARCH_REQUEST_FAILED':
      return Object.assign({}, state, {
        err: true
      })

    default:
      return state;
  }
}

export default searchReducer;
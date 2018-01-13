const initialState = {
  results: null,      // {arr}
  totalResults: 0,    // {int}
  err: null           // {str}
}

const searchReducer = (state = initialState, action) => {
  switch (action.type) {
    case 'SET_SEARCH_QUERY':
      return Object.assign({}, state, {
        query: action.query
      })

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

    case 'LOAD_SEARCH_FROM_OBJECT':
      return Object.assign({}, state, action.obj)

    default:
      return state;
  }
}

export default searchReducer;
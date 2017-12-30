import fetch from 'isomorphic-fetch';

export const receiveSearchResults = (results) => ({
  type: 'RECEIVE_SEARCH_RESULTS', results
})

export const searchRequestFailed = () => ({
  type: 'SEARCH_REQUEST_FAILED'
})

export function fetchSearchResults(query) {
  return function(dispatch) {
    return fetch(query)
      .then(response => response.json()
        .then(json => ({
          status: response.status,
          json
        })
      ))
      .then(({ status, json }) => {
        if (status >= 400) dispatch(searchRequestFailed())
        else dispatch(receiveSearchResults(json))
      }, err => { dispatch(searchRequestFailed()) })
  }
}
import fetch from 'isomorphic-fetch';

export const setTypeaheadField = (field) => ({
  type: 'SET_TYPEAHEAD_FIELD', field
})

export const setTypeaheadQuery = (query) => ({
  type: 'SET_TYPEAHEAD_QUERY', query
})

export const setTypeaheadIndex = (index) => ({
  type: 'SET_TYPEAHEAD_INDEX', index
})

export const receiveTypeaheadResults = (results) => ({
  type: 'RECEIVE_TYPEAHEAD_RESULTS', results
})

export const typeaheadRequestFailed = () => ({
  type: 'TYPEAHEAD_REQUEST_FAILED'
})

export function fetchTypeaheadResults(query) {
  return function(dispatch) {
    return fetch(query)
      .then(response => response.json()
        .then(json => ({ status: response.status, json })
      ))
      .then(({ status, json }) => {
        if (status >= 400) dispatch(typeaheadRequestFailed())
        else dispatch(receiveTypeaheadResults(json))
      }, err => { dispatch(typeaheadRequestFailed()) })
  }
}
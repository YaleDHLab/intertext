import fetch from 'isomorphic-fetch';
import config from '../../server/config';
import { setTypeaheadIndex } from './typeahead';

export const receiveSearchResults = (results) => ({
  type: 'RECEIVE_SEARCH_RESULTS', results
})

export const searchRequestFailed = () => ({
  type: 'SEARCH_REQUEST_FAILED'
})

export const fetchSearchResults = () => {
  return (dispatch, getState) => {
    // Reset the typeahead index given new results
    dispatch(setTypeaheadIndex(0))
    // Generate the query url
    const query = getSearchUrl(getState());
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

export const getSearchUrl = (state) => {
  let url = config.endpoint + 'clustered_matches?limit=1000';
  const query = encodeURIComponent(state.typeahead.query);
  const field = state.typeahead.field.toLowerCase();
  if (state.useTypes.previous) {
    url += '&source_' + field + '=' + query;
  }
  if (state.useTypes.later) {
    url += '&target_' + field + '=' + query;
  }
  if (state.similarity.similarity) {
    url += '&min_similarity=' + state.similarity.similarity[0];
    url += '&max_similarity=' + state.similarity.similarity[1];
  }
  if (state.sort && state.sort != 'Sort By') {
    url += '&sort=' + state.sort;
  }
  return url;
}
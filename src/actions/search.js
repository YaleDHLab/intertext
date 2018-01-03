import fetch from 'isomorphic-fetch';
import config from '../../server/config';
import { history } from '../store';
import { setSort } from './sort-results';
import { setUseTypes } from './use-types';
import { setTypeaheadField, setTypeaheadQuery,
  setTypeaheadIndex} from './typeahead';
import { setDisplayed, setSimilarity } from './similarity-slider';

export const receiveSearchResults = (results) => ({
  type: 'RECEIVE_SEARCH_RESULTS', results
})

export const searchRequestFailed = () => ({
  type: 'SEARCH_REQUEST_FAILED'
})

export const fetchSearchResults = () => {
  return (dispatch, getState) => {
    // Save the user's search in the url
    dispatch(saveSearchInUrl())
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

export const saveSearchInUrl = () => {
  return (dispatch, getState) => {
    const _state = getState();
    let hash = 'results?store=true';
    hash += '&query=' + JSON.stringify(_state.typeahead.query);
    hash += '&sort=' + JSON.stringify(_state.sort);
    hash += '&similarity=' + JSON.stringify(_state.similarity.similarity);
    hash += '&displayed=' + JSON.stringify(_state.similarity.displayed);
    hash += '&field=' + JSON.stringify(_state.typeahead.field);
    hash += '&useTypes=' + JSON.stringify(_state.useTypes);
    try { history.push(hash) } catch(err) {}
  }
}

export const loadSearchFromUrl = (obj) => {
  return (dispatch, getState) => {
    if (!obj) return;
    let _state = getState();
    obj.substring(1).split('&').map((arg) => {
      const split = arg.split('=');
      _state = Object.assign({}, _state, {
        [split[0]]: JSON.parse( decodeURIComponent( split[1] ) )
      })
    })
    dispatch(setSort(_state.sort))
    dispatch(setDisplayed(_state.displayed))
    dispatch(setSimilarity(_state.similarity))
    dispatch(setUseTypes(_state.useTypes))
    dispatch(setTypeaheadField(_state.field))
    dispatch(setTypeaheadQuery(_state.query))
    dispatch(fetchSearchResults())
  }
}
import fetch from 'isomorphic-fetch';
import config from '../../server/config';
import { history } from '../store';
import { setSort } from './sort-results';
import { setUseTypes } from './use-types';
import { setCompare } from './compare';
import { setDisplayed, setSimilarity } from './similarity-slider';
import {
  setTypeaheadField,
  setTypeaheadQuery,
  setTypeaheadIndex
} from './typeahead';

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
        .then(json => ({ status: response.status, json })
      ))
      .then(({ status, json }) => {
        if (status >= 400) dispatch(searchRequestFailed())
        else dispatch(receiveSearchResults(json))
      }, err => { dispatch(searchRequestFailed()) })
  }
}

export const getSearchUrl = (state) => {
  let url = config.endpoint + 'matches?',
      query = '',
      field = '';
  if (!state.typeahead && !state.field) return url;

  if (state.typeahead.query) {
    query = encodeURIComponent(state.typeahead.query);
  }
  if (state.typeahead.field) {
    field = state.typeahead.field.toLowerCase();
  }
  if (state.useTypes.previous && field && query) {
    url += '&source_' + field + '=' + query;
  }
  if (state.useTypes.later && field && query) {
    url += '&target_' + field + '=' + query;
  }
  if (state.similarity.similarity) {
    url += '&min_similarity=' + state.similarity.similarity[0];
    url += '&max_similarity=' + state.similarity.similarity[1];
  }
  if (state.sort && state.sort != 'Sort By') {
    url += '&sort=' + state.sort;
  }
  if (state.compare.file_id && state.compare.segment_ids && state.compare.type) {
    url += '&' + state.compare.type + '_file_id=' + state.compare.file_id;
    url += '&' + state.compare.type + '_segment_ids=' + state.compare.segment_ids;
  }
  url += 'limit=1000';
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
    hash += '&compare=' + JSON.stringify(_state.compare);
    try { history.push(hash) } catch(err) {}
  }
}

export const loadSearchFromUrl = (str) => {
  return (dispatch, getState) => {
    if (!str) return; // str should be window.location.search
    if (str.includes('unit=')) return; // skip scatterlot urls
    let _state = getState();
    str.substring(1).split('&').map((arg) => {
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
    dispatch(setCompare(_state.compare))
    dispatch(fetchSearchResults())
  }
}
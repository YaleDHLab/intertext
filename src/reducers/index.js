import { combineReducers } from 'redux';
import waffleReducer from './waffleReducer';
import searchReducer from './searchReducer';
import compareReducer from './compareReducer';
import naviconReducer from './naviconReducer';
import useTypesReducer from './useTypesReducer';
import favoriteReducer from './favoriteReducer';
import typeaheadReducer from './typeaheadReducer';
import similarityReducer from './similarityReducer';
import scatterplotReducer from './scatterplotReducer';
import sortResultsReducer from './sortResultsReducer';

export const rootReducer = combineReducers({
  waffle: waffleReducer,
  search: searchReducer,
  compare: compareReducer,
  navicon: naviconReducer,
  useTypes: useTypesReducer,
  favorites: favoriteReducer,
  typeahead: typeaheadReducer,
  similarity: similarityReducer,
  scatterplot: scatterplotReducer,
  sort: sortResultsReducer,
});
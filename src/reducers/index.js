import { combineReducers } from 'redux';
import searchReducer from './searchReducer';
import naviconReducer from './naviconReducer';
import useTypesReducer from './useTypesReducer';
import typeaheadReducer from './typeaheadReducer';
import similarityReducer from './similarityReducer';
import sortResultsReducer from './sortResultsReducer';

export const rootReducer = combineReducers({
  search: searchReducer,
  navicon: naviconReducer,
  useTypes: useTypesReducer,
  typeahead: typeaheadReducer,
  similarity: similarityReducer,
  sort: sortResultsReducer
});
import { combineReducers } from 'redux';
import naviconReducer from './naviconReducer';
import typeaheadReducer from './typeaheadReducer';
import searchReducer from './searchReducer';

export const rootReducer = combineReducers({
  navicon: naviconReducer,
  typeahead: typeaheadReducer,
  search: searchReducer,
});
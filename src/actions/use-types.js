import { fetchSearchResults } from './search';

export const toggleUseTypes = (use) => {
  return (dispatch) => {
    dispatch({type: 'TOGGLE_USE_TYPES', use: use})
    dispatch(fetchSearchResults())
  }
}
import { fetchSearchResults } from './search';

export const setUseTypes = (obj) => ({
  type: 'SET_USE_TYPES', obj: obj
})

export const toggleUseTypes = (use) => {
  return (dispatch) => {
    dispatch({type: 'TOGGLE_USE_TYPES', use: use})
    dispatch(fetchSearchResults())
  }
}
import { fetchSearchResults } from './search';

export const setSortField = (field) => {
  return (dispatch) => {
    dispatch({type: 'SET_SORT_FIELD', field: field});
    dispatch(fetchSearchResults())
  }
}
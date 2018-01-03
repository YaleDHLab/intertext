import { fetchSearchResults } from './search';

export const setSort = (field) => ({
  type: 'SET_SORT', field: field
})

export const setSortAndSearch = (field) => {
  return (dispatch) => {
    dispatch(setSort(field));
    dispatch(fetchSearchResults());
  }
}
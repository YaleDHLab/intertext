import { fetchSearchResults } from './search';

export const setDisplayed = (val) => ({
  type: 'SET_DISPLAYED', val: val
})

export const setSimilarity = (val) => {
  return (dispatch) => {
    dispatch({type: 'SET_SIMILARITY', val: val})
    dispatch(fetchSearchResults())
  }
}
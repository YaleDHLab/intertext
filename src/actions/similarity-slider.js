import { fetchSearchResults } from './search';

export const setDisplayed = (val) => ({
  type: 'SET_DISPLAYED', val: val
})

export const setSimilarity = (val) => ({
  type: 'SET_SIMILARITY', val: val
})

export const setSimilarityAndSearch = (val) => {
  return (dispatch) => {
    dispatch(setSimilarity(val))
    dispatch(fetchSearchResults())
  }
}
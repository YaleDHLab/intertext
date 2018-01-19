export const sort = (arr) => Object.assign([], arr).sort((a, b) => a - b)

export const toggleFavorite = (obj) => {
  return (dispatch, getState) => {
    const _id = obj.result._id;
    const _state = getState();
    if (_state.favorites[obj.type].indexOf(_id) > -1) {
      dispatch(removeFavorite({type: obj.type, _id: _id}))
    } else {
      dispatch(addFavorite({type: obj.type, _id: _id}))
    }
    dispatch(saveFavorites())
  }
}

export const addFavorite = (obj) => ({
  type: 'ADD_FAVORITE', obj: obj
})

export const removeFavorite = (obj) => ({
  type: 'REMOVE_FAVORITE', obj: obj
})

export const saveFavorites = () => {
  return (dispatch, getState) => {
    try {
      localStorage.setItem('favorites', JSON.stringify(getState().favorites));
    } catch(err) { console.warn(err) }
  }
}

export const loadFavorites = () => {
  return (dispatch, getState) => {
    const favorites = localStorage.getItem('favorites')
    if (favorites) {
      dispatch({'type': 'LOAD_FAVORITES', obj: JSON.parse(favorites)})
    }
  }
}
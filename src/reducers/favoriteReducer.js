const initialState = {
  source: [],
  target: []
}

const favoriteReducer = (state = initialState, action) => {
  switch (action.type) {
    case 'ADD_FAVORITE':
      return Object.assign({}, state, {
        [action.obj.type]: [...state[action.obj.type], action.obj.matchIds]
      })
    case 'REMOVE_FAVORITE':
      return Object.assign({}, state, {
        [action.obj.type]: state[action.obj.type]
            .filter(m => m != action.obj.matchIds)
      })
    case 'LOAD_FAVORITES':
      return Object.assign({}, state, action.obj)
    default:
      return state;
  }
}

export default favoriteReducer;
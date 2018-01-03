const initialState = {
  previous: true,
  later: false
}

const useTypesReducer = (state = initialState, action) => {
  switch (action.type) {
    case 'SET_USE_TYPES':
      return Object.assign({}, state, action.obj)

    case 'TOGGLE_USE_TYPES':
      const otherUse = action.use === 'previous' ? 'later' : 'previous';
      // ensure at least one use is active
      return state[action.use] && !state[otherUse] ?
          Object.assign({}, state, {
            [otherUse]: true,
            [action.use]: false
          })
        : Object.assign({}, state, {
            [action.use]: !state[action.use]
          })

    default:
      return state;
  }
}

export default useTypesReducer;
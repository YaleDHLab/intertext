const initialState = {};

const compareReducer = (state = initialState, action) => {
  switch (action.type) {
    case 'START_COMPARE':
      return Object.assign({}, state, {
        type: action.obj.type,
        file_id: action.obj.file_id,
        segment_ids: action.obj.segment_ids,
      })
    case 'END_COMPARE':
      return {};
    case 'SET_COMPARE':
      return Object.assign({}, action.obj)
    default:
      return state;
  }
}

export default compareReducer;
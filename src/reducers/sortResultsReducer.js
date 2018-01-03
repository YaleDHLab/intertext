const initialState = 'Sort By';

const sortResultsReducer = (state = initialState, action) => {
  switch (action.type) {
    case 'SET_SORT_FIELD':
      return action.field;

    default:
      return state;
  }
}

export default sortResultsReducer;
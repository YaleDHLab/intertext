const naviconReducer = (state = 'closed', action) => {
  switch (action.type) {
    case 'OPEN_NAVICON':
      return 'open';
    case 'CLOSE_NAVICON':
      return 'closed';
    default:
      return state;
  }
}

export default naviconReducer;
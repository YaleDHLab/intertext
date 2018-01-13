const initialState = {
  feature: 'author', // 'author' || 'segment_ids' || 'year'
  data: [], 
  xDomain: [], 
  width: 0, // width of the chart
  columnCounts: {},
  active: null, // the waffle cell the user clicked on most recently
  
  type: '', // 'source' || 'target'
  file_id: '', // file_id of the selected passage
  author: '', // author of the selected passage
  title: '', // title of the selected passage
  image: null, // path to image of author of the selected passage
  err: null,
}

const waffleReducer = (state = initialState, action) => {
  switch (action.type) {
    case 'SET_WAFFLE_VISIBILITY':
      return Object.assign({}, state, {
        displayed: action.visible,
      })

    case 'SET_WAFFLE_FEATURE':
      return Object.assign({}, state, {
        feature: action.feature,
      })

    case 'WAFFLE_REQUEST_FAILED':
      return Object.assign({}, state, {
        err: true,
      })

    case 'RECEIVE_WAFFLE_IMAGE':
      return Object.assign({}, state, {
        image: action.url,
      })

    case 'SET_WAFFLE_VISUALIZED':
      return Object.assign({}, state, {
        type: action.obj.type,
        title: action.obj[ action.obj.type + '_title' ],
        file_id: action.obj[ action.obj.type + '_file_id' ],
        author: action.obj[ action.obj.type + '_author' ],
        active: null,
      })

    case 'SET_PROCESSED_DATA':
      return Object.assign({}, state, {
        data: action.obj.data,
        xDomain: action.obj.xDomain,
        feature: action.obj.feature,
        width: action.obj.width,
        columnCounts: action.obj.columnCounts,
        maxColumn: action.obj.maxColumn,
        levelMargin: action.obj.levelMargin,
      })

    case 'SET_WAFFLE_ACTIVE':
      return Object.assign({}, state, {
        active: action.obj,
      })

    default:
      return state;
  }
}

export default waffleReducer;
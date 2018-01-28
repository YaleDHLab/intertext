const initialState = {
  unit: 'passage', // 'passage', 'author', or 'book'
  data: [], // the filtered data shown in view
  use: 'earlier', // 'earlier' or 'later'
  statistic: 'mean', // 'sum' or 'mean'
  xDomain: [], // list of min, max similarity floats
  yDomain: [], // list of min, max year ints
  y: 'target_year', // attribute displayed on y axis
  zoomed: false,
  jitter: false,
  tooltip: {name: '', title: '', x: '', y: ''},
  err: false
}

const scatterplotReducer = (state = initialState, action) => {
  switch (action.type) {
    case 'SET_UNIT':
      return Object.assign({}, state, {
        unit: action.unit,
      })

    case 'SET_USE':
      return Object.assign({}, state, {
        use: action.use,
        y: action.use === 'earlier' ? 'target_year' : 'source_year',
      })

    case 'SET_STATISTIC':
      return Object.assign({}, state, {
        statistic: action.stat
      })

    case 'SET_Y':
      return Object.assign({}, state, {
        y: action.y,
      })

    case 'SET_TOOLTIP':
      return Object.assign({}, state, {
        tooltip: action.obj,
      })

    case 'TOGGLE_JITTER':
      return Object.assign({}, state, {
        jitter: !state.jitter,
      })

    case 'SET_DISPLAYED_DOMAINS':
      return Object.assign({}, state, {
        xDomain: action.obj.x,
        yDomain: action.obj.y,
        zoomed: true,
      })

    case 'REMOVE_ZOOM':
      return Object.assign({}, state, {
        xDomain: [],
        yDomain: [],
        zoomed: false,
      })

    case 'SCATTERPLOT_REQUEST_FAILED':
      return Object.assign({}, state, {
        err: true,
      })

    case 'RECEIVE_SCATTERPLOT_RESULTS':
      return Object.assign({}, state, {
        data: action.obj.data,
        xDomain: action.obj.xDomain,
        yDomain: action.obj.yDomain,
        zoomed: action.obj.zoomed,
        statistic: action.obj.statistic || state.statistic,
      })

    default:
      return state;
  }
}

export default scatterplotReducer;
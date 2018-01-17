const initialState = {
  unit: 'passage', // 'passage', 'author', or 'book'
  data: [], // the filtered data shown in view
  allData: [], // all data to be shown in view
  use: 'earlier', // 'earlier' or 'later'
  statistic: 'mean', // 'sum' or 'mean'
  shownXDomain: [], // displayed x domain
  shownYDomain: [], // displayed y domain
  fullXDomain: [], // full, unzoomed x domain
  fullYDomain: [], // full, unzoomed y domain
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

    case 'SET_FULL_X_DOMAIN':
      return Object.assign({}, state, {
        fullXDomain: action.arr, 
      })

    case 'SET_FULL_Y_DOMAIN':
      return Object.assign({}, state, {
        fullYDomain: action.arr,
      })

    case 'SET_SHOWN_X_DOMAIN':
      return Object.assign({}, state, {
        shownXDomain: action.arr,
      })

    case 'SET_SHOWN_Y_DOMAIN':
      return Object.assign({}, state, {
        shownYDomain: action.arr,
      })

    case 'REMOVE_ZOOM':
      return Object.assign({}, state, {
        zoomed: false,
      })

    case 'SCATTERPLOT_REQUEST_FAILED':
      return Object.assign({}, state, {
        err: true,
      })

    case 'RECEIVE_SCATTERPLOT_RESULTS':
      return Object.assign({}, state, {
        data: action.results,
        allData: action.results,
      })

    default:
      return state;
  }
}

export default scatterplotReducer;
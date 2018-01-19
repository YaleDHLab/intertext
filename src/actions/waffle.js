import { history } from '../store';
import fetch from 'isomorphic-fetch';
import config from '../../server/config';
import { fetchSearchResults } from './search';

export const setWaffleVisualized = (obj) => ({
  type: 'SET_WAFFLE_VISUALIZED', obj: obj
})

export const setProcessedData = (obj) => ({
  type: 'SET_PROCESSED_DATA', obj: obj
})

export const setActiveWaffle = (obj) => ({
  type: 'SET_WAFFLE_ACTIVE', obj: obj
})

export const waffleRequestFailed = () => ({
  type: 'WAFFLE_REQUEST_FAILED'
})

export const receiveWaffleImage = (url) => ({
  type: 'RECEIVE_WAFFLE_IMAGE', url: url
})

export const hideWaffle = () => {
  return (dispatch) => {
    dispatch(fetchSearchResults())
  }
}

export const setWaffleFeature = (feature) => {
  return (dispatch) => {
    dispatch({type: 'SET_WAFFLE_FEATURE', feature: feature})
    dispatch(requestWaffleData())
  }
}

export const getWaffleActive = (d, i) => {
  return (dispatch) => {
    dispatch(requestWaffleActiveData(d, i))
  }
}

export const visualize = (obj) => {
  return (dispatch, getState) => {
    const _state = getState()
    dispatch(setWaffleVisualized(obj))
    dispatch(requestWaffleImage())
    dispatch(requestWaffleData())
    dispatch(saveWaffleInUrl())
  }
}

export const saveWaffleInUrl = () => {
  // TODO: Store waffle state in url params 
  return (dispatch) => {
    history.push('waffle')
  }
}

export const requestWaffleImage = () => {
  return (dispatch, getState) => {
    const _state = getState()
    const url = config.endpoint + 'metadata?file_id=' + _state.waffle.file_id;
    return fetch(url)
      .then(response => response.json()
        .then(json => ({ status: response.status, json })
      ))
      .then(({ status, json }) => {
        if (status >= 400) dispatch(waffleRequestFailed())
        else {
          const metadata = json.docs[0].metadata;
          const imageUrl = metadata ? metadata.image : ''
          dispatch(receiveWaffleImage(imageUrl))
        }
      }, err => { dispatch(waffleRequestFailed()) })
  }
}

export const requestWaffleData = () => {
  return (dispatch, getState) => {
    return fetch(getWaffleUrl(getState))
      .then(response => response.json()
        .then(json => ({ status: response.status, json })
      ))
      .then(({ status, json }) => {
        if (status >= 400) dispatch(waffleRequestFailed())
        else dispatch(receiveWaffleData(json.docs || []))
      }, err => { dispatch(waffleRequestFailed()) })
  }
}

export const requestWaffleActiveData = (d) => {
  return (dispatch) => {
    if (!d._id) {console.warn('waffle request missing _id'); return;};
    const url = config.endpoint + 'matches?_id=' + d._id;
    return fetch(url)
      .then(response => response.json()
        .then(json => ({ status: response.status, json })
      ))
      .then(({ status, json }) => {
        if (status >= 400) dispatch(waffleRequestFailed())
        else dispatch(setActiveWaffle(json.docs[0] || {}))
      }, err => { dispatch(waffleRequestFailed()) })
  }
}

export const receiveWaffleData = (docs) => {
  const size = 10; // h, w of each waffle cell
  const levelMargin = 10; // margin between levels
  const margin = {top: 0, right: 80, bottom: 90, left: 0};
  return (dispatch, getState) => {
    const _state = getState();
    const data = getCellData(_state, docs)
    const cols = data.cols;
    // find the level with the max column count
    const maxCol = keys(cols).reduce((a, b) => (cols[a] > cols[b]) ? a : b);
    // find all unique levels for an ordinal x domain
    const xDomain = keys(cols);
    // find the chart width
    let width = xDomain.length * (cols[maxCol] * size + levelMargin);
    width += margin.right + margin.left;
    dispatch(setProcessedData({
      data: data.cells,
      xDomain: xDomain,
      feature: _state.waffle.feature,
      width: width,
      columnCounts: cols,
      maxColumn: cols[maxCol],
      levelMargin: levelMargin,
    }))
  }
}

const getCellData = (_state, _data) => {
  const rows = 10; // waffles per col
  const key = getKey(_state.waffle.feature, _state.waffle.type);
  let counts = {}, cols = {}, data = [];
  _data.map((d, i) => {
    // level of passage in `feature` factor
    let level = getLevel(d, key, _state.waffle.feature);
    // set the 0-based count of the times each level occurs
    counts[level] = counts[level] > -1 ? counts[level] + 1 : 0;
    // set the 0-based column index where this cell belongs
    let col = Math.floor(counts[level] / rows);
    // set the 0-based row index where this cell belongs
    let row = (counts[level] % rows);
    // set the 1-based count of columns for this level
    cols[level] = cols[level] ? Math.max(cols[level], col + 1) : 1;
    // add this observation to the outgoing data
    data.push({
      row: row,
      column: col,
      xLevel: level,
      similarity: d.similarity,
      _id: d._id,
    })
  })
  return {cells: data, cols: cols};
}

const keys = (obj) => Object.keys(obj);

const getWaffleUrl = (getState) => {
  const waffle = getState().waffle;
  return config.endpoint + 'matches?' +
    waffle.type + '_file_id=' + waffle.file_id + '&limit=500';
}

const getKey = (feature, type) => {
  const otherType = type === 'source' ? 'target' : 'source';
  return feature === 'segment_ids' ?
    type + '_segment_ids' : otherType + '_' + feature;
}

const getLevel = (d, key, feature) => {
  if (!d[key]) { console.warn('level data is unreachable'); return; };
  return feature === 'segment_ids' ? d[key][0] : d[key];
}
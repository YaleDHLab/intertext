import fetch from 'isomorphic-fetch';
import { getSearchUrl } from './search';
import * as d3 from 'd3';

export const toggleJitter = () => ({
  type: 'TOGGLE_JITTER',
})

export const removeZoom = () => ({
  type: 'REMOVE_ZOOM',
})

export const setY = (y) => ({
  type: 'SET_Y', y: y,
})

export const requestFailed = () => ({
  type: 'SCATTERPLOT_REQUEST_FAILED',
})

export const receiveResults = (results) => ({
  type: 'RECEIVE_SCATTERPLOT_RESULTS', results: results,
})

export const setShownXDomain = (arr) => ({
  type: 'SET_SHOWN_X_DOMAIN', arr: arr,
})

export const setShownYDomain = (arr) => ({
  type: 'SET_SHOWN_Y_DOMAIN', arr: arr,
})

export const setFullXDomain = (arr) => ({
  type: 'SET_FULL_X_DOMAIN', arr: arr,
})

export const setFullYDomain = (arr) => ({
  type: 'SET_FULL_Y_DOMAIN', arr: arr,
})

export const setTooltip = (obj) => ({
  type: 'SET_TOOLTIP', obj: obj,
})

export const setDisplayedDomains = (obj) => {
  return (dispatch) => {
    dispatch({type: 'SET_DISPLAYED_DOMAINS', obj: obj})
    dispatch(fetchScatterplotResults())
  }
}

export const resetZoom = () => {
  return (dispatch, getState) => {
    const _state = getState();
    dispatch(setDisplayedDomains(_state.fullDomains))
    dispatch(removeZoom())
    dispatch(fetchScatterplotResults())
  }
}

export const setUnit = (unit) => {
  return (dispatch) => {
    dispatch({type: 'SET_UNIT', unit: unit})
    dispatch(fetchScatterplotResults())
  }
}

export const setStatistic = (stat) => {
  return (dispatch) => {
    dispatch({type: 'SET_STATISTIC', stat: stat})
    dispatch(fetchScatterplotResults())
  }
}

export const setUse = (use) => {
  return (dispatch) => {
    dispatch({type: 'SET_USE', use: use})
    dispatch(fetchScatterplotResults())
  }
}

export const fetchScatterplotResults = () => {
  return (dispatch, getState) => {
    const query = getSearchUrl({});
    const _state = getState().scatterplot;
    return fetch(query + 'limit=500')
      .then(response => response.json()
        .then(json => ({ status: response.status, json })
      ))
      .then(({ status, json }) => {
        if (status >= 400) dispatch(scatterplotRequestFailed())
        else dispatch(receiveResults(parseResults(json.docs, _state, dispatch)))
      }, err => { dispatch(requestFailed()) })
  }
}

const keys = (obj) => Object.keys(obj);

const sum = (arr) => arr.reduce((a, b) => a + b, 0);

const mean = (arr) => sum(arr) / arr.length;

const sort = (arr, k) => arr.sort((a, b) => a[k] - b[k]);

const sortKeys = (obj) => keys(obj).sort((a, b) => obj[a] - obj[b]).reverse();

const hasKey = (obj, k) => obj.hasOwnProperty(k)

const nest = (arr, k) => {
  let obj = {};
  arr.map((i) => {
    hasKey(obj, i[k]) ? obj[ i[k] ].push(i) : obj[ i[k] ] = [i]
  })
  return obj;
}

const getLevel = (_state) => {
  const use = getUse(_state);
  switch (_state.unit) {
    case 'book':
      return use + '_file_id';
    case 'author':
      return use + '_author';
    case 'passage':
      return use + '_segment_ids';
  }
}

const getUse = (_state) => _state.use === 'earlier' ? 'target' : 'source';

const parseResults = (docs, _state, dispatch) => {
  const use = getUse(_state)
  const level = getLevel(_state)
  const dataNest = nest(docs, level)
  const simsByLevel = getSimsByLevel(dataNest)
  const simByLevel = getSimByLevel(simsByLevel, _state)
  // sort keys into descending order of similarity
  const sorted = sortKeys(simByLevel);
  // build the data used for the visualization
  let data = [];
  sorted.map((k, idx) => {
    // pluck attributes from the first observation from this level
    const o = dataNest[k][0];
    data.push({
      key: Array.isArray(o[level]) ? o[level].join('.') : o[level],
      similarity: simByLevel[k],
      title: o[use + '_title'],
      author: o[use + '_author'],
      match: o[use + '_match'],
      source_year: parseInt(o['source_year']),
      target_year: parseInt(o['target_year']),
      label: idx < 20 ? idx + 1 : null,
    })
  })
  setDomains(_state, dispatch, data)
  return data;
}

// find all similarity values for each level of the visualization
const getSimsByLevel = (dataNest) => {
  let simsByLevel = {};
  keys(dataNest).map((k) => {
    dataNest[k].map((v) => {
      if (hasKey(simsByLevel, k)) simsByLevel[k].push(v.similarity)
      else simsByLevel[k] = [v.similarity] 
    })
  })
  return simsByLevel;
}

// find one statistic for each level of the visualization
const getSimByLevel = (simsByLevel, _state) => {
  return keys(simsByLevel).reduce((obj, k) => {
    if (_state.statistic === 'sum') obj[k] = sum(simsByLevel[k])
    else obj[k] = mean(simsByLevel[k])
    return obj;
  }, {})
}

// set the full and displayed domains
const setDomains = (_state, dispatch, data) => {
  const use = getUse(_state)
  dispatch( setShownXDomain(d3.extent(data, (d) => d.similarity)) )
  dispatch( setShownYDomain(d3.extent(data, (d) => d[use + '_year'])) )
  dispatch( setFullXDomain(d3.extent(data, (d) => d.similarity)) )
  dispatch( setFullYDomain(d3.extent(data, (d) => d[use + '_year'])) )
}
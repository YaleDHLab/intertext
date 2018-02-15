import fetch from 'isomorphic-fetch';
import config from '../../server/config';
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

export const receiveResults = (obj) => ({
  type: 'RECEIVE_SCATTERPLOT_RESULTS', obj: obj,
})

export const setTooltip = (obj) => ({
  type: 'SET_TOOLTIP', obj: obj,
})

export const setDomains = (domains) => {
  return (dispatch, getState) => {
    dispatch(fetchScatterplotResults({domains: domains}))
  }
}

export const resetZoom = () => {
  return (dispatch) => {
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
    dispatch(fetchScatterplotResults({statistic: stat}))
  }
}

export const setUse = (use) => {
  return (dispatch) => {
    dispatch({type: 'SET_USE', use: use})
    dispatch(fetchScatterplotResults())
  }
}

export const fetchScatterplotResults = (options) => {
  options = options || {};
  return (dispatch, getState) => {
    const _state = getState().scatterplot;
    return fetch(getScatterplotUrl(_state, options))
      .then(response => response.json()
        .then(json => ({ status: response.status, json })
      ))
      .then(({ status, json }) => {
        if (status >= 400) dispatch(scatterplotRequestFailed())
        else {
          dispatch(parseResults(json.docs, options))
        }
      }, err => { dispatch(requestFailed()) })
  }
}

const getScatterplotUrl = (state, options) => {
  const use = getUse(state.use);
  const unit = getUnit(state.unit);
  let url = config.endpoint + 'scatterplot';
  url += '?limit=500';
  url += '&type=' + use;
  url += '&unit=' + unit;
  if (options.statistic) {
    url += '&statistic=' + options.statistic;
  } else {
    url += '&statistic=' + state.statistic;
  }
  if (options.domains && options.domains.x) {
    url += '&min_similarity=' + options.domains.x[0];
    url += '&max_similarity=' + options.domains.x[1];
  }
  if (options.domains && options.domains.y) {
    url += '&min_' + use + '_year=' + Math.floor(options.domains.y[0]);
    url += '&max_' + use + '_year=' + Math.floor(options.domains.y[1]);
  }
  return url;
}

const getUse = (use) => use === 'earlier' ? 'target' : 'source';

const getUnit = (unit) => {
  switch (unit) {
    case 'passage':
      return 'segment_ids';
    case 'author':
      return 'author';
    case 'book':
      return 'file_id';
  }
}

// set the full and displayed domains
const getDomains = (data, _state) => {
  return {
    x: d3.extent(data, (d) => d.similarity),
    y: d3.extent(data, (d) => d[getUse(_state.use) + '_year']),
  }
}

const parseResults = (data, options) => {
  return (dispatch, getState) => {
    const domains = getDomains(data, getState().scatterplot);
    for (let i=0; i<20; i++) {
      try { data[i].label = i+1; } catch (e) {}
    }
    let args = {
      data: data,
      xDomain: domains.x,
      yDomain: domains.y,
      zoomed: options.domains ? true : false,
    }
    if (options.statistic) {
      args.statistic = options.statistic
    };
    dispatch(receiveResults(args))
  }
}
import { createStore, applyMiddleware, compose } from 'redux';
import { routerMiddleware, connectRouter } from 'connected-react-router';
import { createBrowserHistory } from 'history';
import { createLogger } from 'redux-logger';
import thunkMiddleware from 'redux-thunk';
import { rootReducer } from './reducers/index';
import { loadSearchFromUrl } from './actions/search';
import freeze from 'redux-freeze';

const history = createBrowserHistory();
const loggerMiddleware = createLogger()

let middlewares = [
  thunkMiddleware,
  routerMiddleware(history),
]

// add the freeze dev middleware
if (process.env.NODE_ENV !== 'production') {
  middlewares.push(freeze)
  middlewares.push(loggerMiddleware)
}

// apply the middleware
let middleware = applyMiddleware(...middlewares);

// add the redux dev tools
if (process.env.NODE_ENV !== 'production') {
  if (window.devToolsExtension) {
    middleware = compose(middleware, window.devToolsExtension())
  } else if (window.__REDUX_DEVTOOLS_EXTENSION_COMPOSE__) {
    middleware = composeEnhancer( applyMiddleware(middleware) )
  }
}

// create the store
const store = createStore(
  connectRouter(history)(rootReducer),
  middleware
);

store.dispatch(loadSearchFromUrl(window.location.search));

export { store, history };
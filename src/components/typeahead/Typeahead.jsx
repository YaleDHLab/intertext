import React from 'react';
import Select from './Select';
import Results from './Results';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import config from '../../../server/config';
import { withRouter } from 'react-router-dom';
import { fetchSearchResults } from '../../actions/search';
import {
  setTypeaheadQuery,
  setTypeaheadIndex,
  fetchTypeaheadResults,
} from '../../actions/typeahead';

class Typeahead extends React.Component {
  constructor(props) {
    super(props)
    this.handleChange = this.handleChange.bind(this)
    this.handleKeyUp = this.handleKeyUp.bind(this)
    this.submitSearch = this.submitSearch.bind(this)
  }

  componentWillUpdate(nextProps) {
    if (nextProps.query !== this.props.query ||
        nextProps.field !== this.props.field) {
      const query = buildTypeaheadQuery(nextProps);
      this.props.fetchTypeaheadResults(query);
    }
  }

  handleKeyUp(e) {
    var index = this.props.index;
    // up arrow
    if (e.keyCode === 38) {
      if (index - 1 >= 0) {
        this.props.setTypeaheadIndex(index - 1)
      }
    // down arrow
    } else if (e.keyCode === 40) {
      if (index + 1 <= this.props.results.length) {
        this.props.setTypeaheadIndex(index + 1)
      }
    // enter key
    } else if (e.keyCode === 13) {
      this.submitSearch()
    }
  }

  handleChange(e) {
    if (e.keyCode === 38 || e.keyCode === 40 || e.keyCode === 13) return;
    this.props.setTypeaheadQuery(e.target.value);
    this.props.setTypeaheadIndex(0);
  }

  submitSearch() {
    // identify the search phrase requested by the user
    const phrase = this.props.index === 0 ?
        this.props.query
      : this.props.results[this.props.index - 1];
    this.props.setTypeaheadQuery(phrase)
    // submit the search and remove focus from the input
    this.props.fetchSearchResults();
    document.querySelector('.typeahead input').blur()
  }

  render() {
    return (
      <div className='typeahead'>
        <Select />
        <div className='search-button' />
        <input value={this.props.query}
            onKeyUp={this.handleKeyUp}
            onChange={this.handleChange}
            onBlur={this.handleBlur} />
        <Results submitSearch={this.submitSearch} />
      </div>
    )
  }
}

const buildTypeaheadQuery = (props) => {
  // build the url to which the query will be sent
  let url = config.endpoint + 'typeahead' +
    '?field=' + props.field.toLowerCase() +
    '&value=' + props.query;
  if (props.type) url += '&type=' + props.type + '_' + props.field.toLowerCase();
  return url;
}

Typeahead.propTypes = {
  query: PropTypes.string.isRequired,
  field: PropTypes.string.isRequired,
  index: PropTypes.number.isRequired,
  results: PropTypes.array.isRequired,
  setTypeaheadQuery: PropTypes.func.isRequired,
  setTypeaheadIndex: PropTypes.func.isRequired,
  fetchSearchResults: PropTypes.func.isRequired,
  history: PropTypes.shape({
    push: PropTypes.func.isRequired,
  }).isRequired,
}

const mapStateToProps = state => ({
  query: state.typeahead.query,
  field: state.typeahead.field,
  index: state.typeahead.index,
  results: state.typeahead.results,
  type: state.search.type,
})

const mapDispatchToProps = dispatch => ({
  setTypeaheadQuery: (val) => dispatch(setTypeaheadQuery(val)),
  setTypeaheadIndex: (val) => dispatch(setTypeaheadIndex(val)),
  fetchTypeaheadResults: (query) => dispatch(fetchTypeaheadResults(query)),
  fetchSearchResults: () => dispatch(fetchSearchResults())
})

export default withRouter( connect(mapStateToProps, mapDispatchToProps)(Typeahead) )
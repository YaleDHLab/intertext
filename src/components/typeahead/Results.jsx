import React from 'react';
import Select from './Select';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';

const Result = (props) => (
  <span onClick={props.onClick}
    className={props.active ?
      'typeahead-result active'
    : 'typeahead-result'}>{props.val}</span>
)

class Results extends React.Component {
  render() {
    const className = this.props.results.length > 0 ? '' : 'hidden';
    return (
      <span className={'typeahead-results ' + className}>
        {this.props.query.length > 0 ?
            <span className={this.props.index === 0 ?
                  'typeahead-result search-icon active'
                : 'typeahead-result search-icon'}>
              {this.props.query}
            </span>
          : <span />
        }
        {this.props.results.map((result, idx) => {
          return <Result key={idx}
            val={result}
            onClick={this.handleClick}
            active={idx + 1 === this.props.index}
            submitSearch={this.props.submitSearch} />
        })}
      </span>
    )
  }
}

Results.propTypes = {
  results: PropTypes.array.isRequired,
  query: PropTypes.string.isRequired,
  index: PropTypes.number.isRequired,
  submitSearch: PropTypes.func.isRequired
}

const mapStateToProps = state => ({
  results: state.typeahead.results,
  query: state.typeahead.query,
  index: state.typeahead.index
})

export default connect(mapStateToProps)(Results)
import React from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';

class ResultCount extends React.Component {
  render() {
    return (
      <div className='results-count'>
        <span>Your query returned</span>
        <span><b>{this.props.totalResults || 0}</b></span>
        <span>results</span>
      </div>
    )
  }
}

ResultCount.PropTypes = {
  totalResults: PropTypes.number
}

const mapStateToProps = state => ({
  totalResults: state.search.totalResults
})

export default connect(mapStateToProps)(ResultCount)
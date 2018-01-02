import React from 'react';
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

const mapStateToProps = state => ({
  totalResults: state.search.totalResults
})

export default connect(mapStateToProps)(ResultCount)
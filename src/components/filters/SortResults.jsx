import React from 'react';
import { connect } from 'react-redux';
import { setSortAndSearch } from '../../actions/sort-results';

class SortResults extends React.Component {
  constructor(props) {
    super(props)
    this.handleChange = this.handleChange.bind(this)
  }

  handleChange(e) {
    this.props.setSortAndSearch(e.target.value)
  }

  render() {
    return (
      <div className='sort-results'>
        <select value={this.props.sort}
            onChange={this.handleChange}>
          <option value='author'>Author</option>
          <option value='year'>Publication Year</option>
          <option value='similarity'>Similarity</option>
        </select>
      </div>
    )
  }
}

const mapStateToProps = state => ({
  sort: state.sort,
})

const mapDispatchToProps = dispatch => ({
  setSortAndSearch: (field) => dispatch(setSortAndSearch(field)),
})

export default connect(mapStateToProps, mapDispatchToProps)(SortResults)
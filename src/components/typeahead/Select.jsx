import React from 'react';
import Results from './Results';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { setTypeaheadField } from '../../actions/typeahead';

class Select extends React.Component {
  render() {
    return (
      <div className='select-container'>
        <select className='custom-select'
            onChange={this.props.setTypeaheadField}>
          <option value='Author'>Author</option>
          <option value='Title'>Title</option>
        </select>
      </div>
    )
  }
}

Select.propTypes = {
  field: PropTypes.string.isRequired,
  setTypeaheadField: PropTypes.func.isRequired,
}

const mapStateToProps = state => ({
  field: state.typeahead.field,
})

const mapDispatchToProps = dispatch => ({
  setTypeaheadField: (event) => dispatch(setTypeaheadField(event.target.value)),
})

export default connect(mapStateToProps, mapDispatchToProps)(Select)
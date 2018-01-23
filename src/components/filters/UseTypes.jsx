import React from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { toggleUseTypes } from '../../actions/use-types';

class UseTypes extends React.Component {
  constructor(props) {
    super(props)
    this.toggleUse = this.toggleUse.bind(this)
  }

  toggleUse(use) {
    this.props.toggleUseTypes(use)
  }

  render() {
    return (
      <div className='use-type'>
        <Button use={'previous'}
          useTypes={this.props.useTypes}
          toggleUse={this.toggleUse}
          label={'Previous Use'} />
        <Button use={'later'}
          useTypes={this.props.useTypes}
          toggleUse={this.toggleUse}
          label={'Later Use'} />
      </div>
    )
  }
}

const Button = (props) => (
  <div onClick={props.toggleUse.bind(null, props.use)}
      className={props.useTypes[props.use] ?
          'use active'
        : 'use'}>{props.label}</div>
)

Button.propTypes = {
  use: PropTypes.string.isRequired,
  useTypes: PropTypes.object.isRequired,
  toggleUse: PropTypes.func.isRequired,
  label: PropTypes.string.isRequired,
}

UseTypes.propTypes = {
  toggleUseTypes: PropTypes.func.isRequired,
  useTypes: PropTypes.object.isRequired,
}

const mapStateToProps = state => ({
  useTypes: state.useTypes,
})

const mapDispatchToProps = dispatch => ({
  toggleUseTypes: (use) => dispatch(toggleUseTypes(use)),
})

export default connect(mapStateToProps, mapDispatchToProps)(UseTypes)
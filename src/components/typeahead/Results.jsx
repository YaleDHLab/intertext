import React from 'react';
import Select from './Select';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { fetchSearchResults } from '../../actions/search';
import { setTypeaheadQuery } from '../../actions/typeahead';

class Results extends React.Component {
  constructor(props) {
    super(props)
    this.handleMousedown = this.handleMousedown.bind(this)
    this.handleMouseup = this.handleMouseup.bind(this)
  }

  componentDidMount() {
    window.addEventListener('mousedown', this.handleMousedown, false)
  }

  componentWillUnmount() {
    window.removeEventListener('mousedown', this.handleMousedown, false)
  }

  handleMousedown(e) {
    // stop blur of input; event order: mousedown, blur, mouseup
    if (e.target.className === 'typeahead-result') {
      e.stopPropagation()
      e.preventDefault()
    }
  }

  handleMouseup(e) {
    if (!e.target.className.includes('typeahead-result')) return;
    this.props.setTypeaheadQuery(e.target.innerText)
    this.props.fetchSearchResults()
    document.querySelector('.typeahead input').blur()
  }

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
            onClick={this.handleMouseup}
            active={idx + 1 === this.props.index}
            submitSearch={this.props.submitSearch} />
        })}
      </span>
    )
  }
}

Results.propTypes = {
  fetchSearchResults: PropTypes.func.isRequired,
  index: PropTypes.number.isRequired,
  query: PropTypes.string.isRequired,
  results: PropTypes.array.isRequired,
  setTypeaheadQuery: PropTypes.func.isRequired,
  submitSearch: PropTypes.func.isRequired,
}

const Result = (props) => (
  <span onClick={props.onClick}
    className={props.active ?
      'typeahead-result active'
    : 'typeahead-result'}>{props.val}</span>
)

Result.propTypes = {
  active: PropTypes.bool,
  onClick: PropTypes.func.isRequired,
  submitSearch: PropTypes.func.isRequired,
  val: PropTypes.string.isRequired,
}

const mapStateToProps = state => ({
  results: state.typeahead.results,
  query: state.typeahead.query,
  index: state.typeahead.index,
})

const mapDispatchToProps = dispatch => ({
  setTypeaheadQuery: (val) => dispatch(setTypeaheadQuery(val)),
  fetchSearchResults: () => dispatch(fetchSearchResults())
})

export default connect(mapStateToProps, mapDispatchToProps)(Results)
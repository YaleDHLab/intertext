import React from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { toggleFavorite, sort } from '../../actions/favorite';
import { toggleCompare } from '../../actions/compare';
import { visualize } from '../../actions/waffle';
import ReadIcon from './icons/ReadIcon';
import CompareIcon from './icons/CompareIcon';
import FavoriteIcon from './icons/FavoriteIcon';
import VisualizeIcon from './icons/VisualizeIcon';

class Result extends React.Component {
  constructor(props) {
    super(props);
    this.getText = this.getText.bind(this)
    this.favorite = this.favorite.bind(this)
    this.compare = this.compare.bind(this)
    this.visualize = this.visualize.bind(this)
    this.getFavoriteClass = this.getFavoriteClass.bind(this)
  }

  getText(field, prefix) {
    const text = this.props.result[this.props.type + '_' + field];
    prefix = prefix || '';
    return {__html: prefix + text}
  }

  favorite() {
    this.props.toggleFavorite({type: this.props.type, result: this.props.result})
  }

  compare() {
    const props = this.props;
    const duration = 1500;
    const results = document.querySelectorAll('.result-pair');
    const container = document.querySelector('.result-pair-container');
    if (container) { // card view compare action
      fadeCardsOut(results)
      getCompareResults(container, duration, props)
      fadeCardsIn(container, results, duration)
    } else { // waffle view compare action
      props.toggleCompare({ type: props.type, result: props.result })
    }
  }

  visualize() {
    this.props.visualize(Object.assign({}, this.props.result, {
      type: this.props.type
    }))
  }

  getFavoriteClass() {
    const _id = this.props.result._id;
    const favs = this.props.favorites[this.props.type];
    return favs.indexOf(_id) > -1 ? 'favorite active' : 'favorite';
  }

  getCompareClass() {
    const compare = this.props.compare;
    const result = this.props.result;
    const type = this.props.type;
    const segment_ids = sort(result[ type + '_segment_ids' ]).join('.');
    return (compare.type === type &&
        compare.file_id === result[ type + '_file_id' ].toString() &&
        compare.segment_ids === segment_ids) ? 'compare active' : 'compare';
  }

  render() {
    return (
      <div className={'result ' + this.props.type}
          style={{height: this.props.height}}>
        <div className='result-wrapper'>
          <div className='result-top'>
            <div className='result-title'
                dangerouslySetInnerHTML={this.getText('title')} />
            <div className='result-year-container'>
              <div className='result-year'
                  dangerouslySetInnerHTML={this.getText('year')} />
            </div>
          </div>
          <div className='result-body'>
            <div className='result-author'
                dangerouslySetInnerHTML={this.getText('author')} />
            <div className='result-match'>
              <span className='prematch'
                  dangerouslySetInnerHTML={this.getText('prematch')} />
              <span className='match'
                  dangerouslySetInnerHTML={this.getText('match', ' ')} />
              <span className='postmatch'
                  dangerouslySetInnerHTML={this.getText('postmatch', ' ')} />
            </div>
            <div className='white-fade' />
          </div>
        </div>
        <div className='result-footer-container'>
          <div className='result-footer'>
            <div onClick={this.read} className='read'>
              <ReadIcon />
              Read
            </div>
            <div onClick={this.compare} className={this.getCompareClass()}>
              <CompareIcon />
              Compare
            </div>
            <div onClick={this.favorite} className={this.getFavoriteClass()}>
              <FavoriteIcon />
              Favorite
            </div>
            <div onClick={this.visualize} className='visualize'>
              <VisualizeIcon />
              Visualize
            </div>
          </div>
        </div>
      </div>
    )
  }
}

const fadeCardsOut = (results) => {
  for (let i=0; i<results.length; i++) {
    setTimeout(animate.bind(null, results[i], true), i * 30)
  }
}

const getCompareResults = (container, duration, props) => {
  setTimeout(() => {
    container.className = container.className + ' fade-out';
    props.toggleCompare({type: props.type, result: props.result})
  }, duration)
}

const fadeCardsIn = (container, results, duration) => {
  setTimeout(() => {
    container.className = container.className.replace(' fade-out', '');
    for (let i=0; i<results.length; i++) { removeAnimation(results[i]) }
  }, duration + 200)
}

const animate = (elem) => {
  elem.className = elem.className + ' animated';
  const circle = elem.querySelector('.similarity-circle');
  circle.className = circle.className + ' fade-out';
}

const removeAnimation = (elem) => {
  elem.className = elem.className.replace(' animated', '');
  const circle = elem.querySelector('.similarity-circle');
  circle.className = circle.className.replace(' fade-out', '');
}

Result.propTypes = {
  type: PropTypes.string.isRequired,
  result: PropTypes.object.isRequired,
  favorites: PropTypes.object.isRequired,
  compare: PropTypes.object.isRequired,
  toggleFavorite: PropTypes.func.isRequired,
  toggleCompare: PropTypes.func.isRequired,
  visualize: PropTypes.func.isRequired,
}

const mapStateToProps = state => ({
  favorites: state.favorites,
  compare: state.compare,
})

const mapDispatchToProps = dispatch => ({
  toggleFavorite: (obj) => dispatch(toggleFavorite(obj)),
  toggleCompare: (obj) => dispatch(toggleCompare(obj)),
  visualize: (obj) => dispatch(visualize(obj)),
})

export default connect(mapStateToProps, mapDispatchToProps)(Result)
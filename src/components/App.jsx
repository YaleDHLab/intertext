import React from 'react';
import Header from './header/Header';
import Footer from './Footer';

export default class AppWrapper extends React.Component {
  render() {
    return (
      <div className='app-container'>
        <div className='page-wrap'>
          <Header />
          {this.props.children}
          <div className='push' />
        </div>
        <Footer />
      </div>
    )
  }
}
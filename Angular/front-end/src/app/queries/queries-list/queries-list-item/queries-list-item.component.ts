import {Component, Input, EventEmitter, Output, OnInit} from '@angular/core';
import { Query } from '../../queries.model';

@Component({
  selector: 'app-queries-list-item',
  templateUrl: './queries-list-item.component.html',
  styleUrls: ['./queries-list-item.component.css']
})
export class QueriesListItemComponent{
  // Bind this component query from the outside
  @Input() query: Query;
  // To be listenable from the outside
  @Output() querySelected = new EventEmitter<void>();

  constructor() {}

  ngOnInIt() {}

  // Trigger querySelected and call emit without any arguments
  // Function selects what query to display
  onSelected() {
    this.querySelected.emit();
  }
}

import { Component, EventEmitter, Output} from '@angular/core';

@Component({
  selector: 'app-header',
  templateUrl: './header.component.html',
  styleUrls: ['./header.component.css']
})
export class HeaderComponent {
  collapsed = true;
  // To be able to be listened from app.component.html
  @Output() featureSelected = new EventEmitter<string>();
  // String that is passed from outside to 'featureSelected' is emitted. Function selects what to display.
  onSelect(feature: string)
  {
    this.featureSelected.emit(feature);
  }
}

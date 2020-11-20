/* *** App Component ***
   Holds the variable 'loadedFeature' to show what to display.
    - loadedFeature: String to hold the requested tab. Displays 'queries' by default.
    - onNavigate(): Pass the string to the loadedFeature variable.
*/
import { Component } from '@angular/core';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent {
  // Initially displaying 'queries' as default.
  loadedFeature = 'queries';

  // Function sets loadedFeature to the passed string.
  onNavigate(feature: string)
  {
    this.loadedFeature = feature;
  }
}
